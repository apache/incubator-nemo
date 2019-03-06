package org.apache.nemo.offloading.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.apache.nemo.offloading.common.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class LambdaWorkerProxy<I, O> implements OffloadingWorker<I, O> {
  private static final Logger LOG = LoggerFactory.getLogger(LambdaWorkerProxy.class.getName());
  private volatile Channel channel;
  private volatile int dataProcessingCnt = 0;
  private volatile boolean finished = false;
  private final BlockingQueue<OffloadingEvent> endQueue;
  private final ConcurrentMap<Channel, EventHandler<OffloadingEvent>> channelEventHandlerMap;

  //private final OffloadingEncoder inputEncoderFactory;
  //private final OffloadingDecoder outputDecoderFactory;


  //private OffloadingEncoder.Encoder encoder;

  private final OffloadingWorkerFactory offloadingWorkerFactory;

  private final Future<Pair<Channel, OffloadingEvent>> channelFuture;

  // two data stream: when channel is null, we buffer byte in byte array output
  // otherwise, we use byteBufOutputStream directly.
  //private ObjectOutputStream objectOutputStream;
  //private ByteBufOutputStream byteBufOutputStream;

  final ExecutorService channelThread = Executors.newSingleThreadExecutor();

  final ExecutorService reExecutionThread = Executors.newSingleThreadExecutor();

  private final ConcurrentMap<Integer, Optional<O>> resultMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, ByteBuf> pendingData = new ConcurrentHashMap<>();

  private volatile Pair<ByteBuf, Integer> currentProcessingInput = null;

  private final String workerId;

  public LambdaWorkerProxy(final int workerId,
                           final Future<Pair<Channel, OffloadingEvent>> channelFuture,
                           final OffloadingWorkerFactory offloadingWorkerFactory,
                           final ConcurrentMap<Channel, EventHandler<OffloadingEvent>> channelEventHandlerMap,
                           final OffloadingEncoder offloadingEncoder,
                           final OffloadingDecoder<O> outputDecoder) {
    this.workerId = Integer.toString(workerId);
    this.channelFuture = channelFuture;
    this.offloadingWorkerFactory = offloadingWorkerFactory;

    this.channelEventHandlerMap = channelEventHandlerMap;
    this.endQueue = new LinkedBlockingQueue<>();

    /*
    this.inputEncoderFactory = inputEncoderFactory;
    this.outputDecoderFactory = outputDecoderFactory;

    final ByteBuf byteBuffer = Unpooled.directBuffer();
    this.byteBufOutputStream = new ByteBufOutputStream(byteBuffer);
    try {
      byteBufOutputStream.writeInt(OffloadingEvent.Type.DATA.ordinal());
      this.encoder = inputEncoderFactory.create(byteBufOutputStream);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    */

    channelThread.execute(() -> {
      try {
        final Pair<Channel, OffloadingEvent> pair = channelFuture.get();
        channel = pair.left();
        final ByteBuf byteBuf = pair.right().getByteBuf();
        final ByteBufInputStream bis = new ByteBufInputStream(byteBuf);
        final int cnt;
        try {
          cnt = bis.readInt();
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
        dataProcessingCnt = cnt;
        byteBuf.release();

        channelEventHandlerMap.put(channel, new EventHandler<OffloadingEvent>() {
          @Override
          public void onNext(OffloadingEvent msg) {
            switch (msg.getType()) {
              case RESULT:
                final ByteBufInputStream bis = new ByteBufInputStream(msg.getByteBuf());
                try {
                  final int hasInstance = bis.readByte();

                  final Optional<O> optional;
                  if (hasInstance != 0) {
                    final O data = outputDecoder.decode(bis);
                    optional = Optional.of(data);
                  } else {
                    optional = Optional.empty();
                  }

                  final int resultId = bis.readInt();
                  dataProcessingCnt = bis.readInt();

                  if (pendingData.get(resultId) == null) {
                    // invalid output
                    // TODO: re-execute the data
                    reExecutionThread.execute(() -> {
                      for (final Integer dataId : pendingData.keySet()) {
                        if (Constants.enableLambdaLogging) {
                          LOG.info("Re-execution data {} in worker {}", dataId, workerId);
                        }

                        final ByteBuf input = pendingData.get(dataId);
                        input.retain();
                        input.retain();

                        pendingData.put(dataId, input);

                        if (currentProcessingInput != null) {
                          throw new RuntimeException("Current processing input should be null");
                        }
                        currentProcessingInput = Pair.of(input.duplicate(), dataId);

                        if (Constants.enableLambdaLogging) {
                          LOG.info("Write data from worker {}, id: {}", workerId, dataId);
                        }

                        channel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.DATA, input));
                      }
                    });
                  } else {
                    final ByteBuf curInputBuf = currentProcessingInput.left();
                    currentProcessingInput = null;
                    curInputBuf.release();

                    resultMap.put(resultId, optional);
                    pendingData.remove(resultId);

                    if (Constants.enableLambdaLogging) {
                      LOG.info("Receive data id {}, processing cnt: {}, pendingData: {} in worker {}", resultId, dataProcessingCnt,
                        pendingData, workerId);
                    }
                  }

                  msg.getByteBuf().release();
                } catch (IOException e) {
                  e.printStackTrace();
                  throw new RuntimeException();
                }
                break;
              case END:
                if (Constants.enableLambdaLogging) {
                  LOG.info("Receive end");
                }
                msg.getByteBuf().release();
                endQueue.add(msg);
                break;
              default:
                throw new RuntimeException("Invalid type: " + msg);
            }
          }
        });
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });
  }

  /*
  @Override
  public <T> List<T> getResult() {
    //LOG.info("Get result");

    if (channel == null) {
      throw new RuntimeException("Channel should not be null");
    }


    final List<T> result = new ArrayList<>();

    while (endQueue.peek() == null) {
      while (!resultQueue.isEmpty()) {
        result.add((T) resultQueue.poll());
      }

      //LOG.info("Result is empty, but don't receive end message");

      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    //LOG.info("We received end message!");

    while (!resultQueue.isEmpty()) {
      result.add((T) resultQueue.poll());
    }

    return result;
  }
*/

  @Override
  public boolean isReady() {
    return channel != null && pendingData.isEmpty();
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public Channel getChannel() {
    return channel;
  }

  @Override
  public Pair<ByteBuf, Integer> getCurrentProcessingInput() {
    return currentProcessingInput;
  }

  @Override
  public int getDataProcessingCnt() {
    return dataProcessingCnt;
  }

  @Override
  public String getId() {
    return workerId;
  }

  @Override
  public Future<Optional<O>> execute(final ByteBuf input, final int dataId,
                                     final boolean speculative) {
    // for future use (speculative execution)
    input.retain();

    if (!speculative) {
      input.writeInt(dataId);
    } else {
      input.retain();
    }

    pendingData.put(dataId, input);


    if (currentProcessingInput != null) {
      throw new RuntimeException("Current processing input should be null");
    }
    currentProcessingInput = Pair.of(input.duplicate(), dataId);


    if (channel != null) {
      if (Constants.enableLambdaLogging) {
        LOG.info("Write data from worker {}, id: {}", workerId, dataId);
      }

      final ChannelFuture channelFuture = channel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.DATA, input));
      return new Future<Optional<O>>() {

        private Optional<O> result = null;

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
          return false;
        }

        @Override
        public boolean isCancelled() {
          return false;
        }

        @Override
        public boolean isDone() {
          if (result == null) {
            result = resultMap.get(dataId);
            if (result == null) {
              return false;
            } else {
              resultMap.remove(dataId);
              return true;
            }
          } else {
            return true;
          }
        }

        @Override
        public Optional<O> get() throws InterruptedException, ExecutionException {
          while (!isDone()) {
            Thread.sleep(200);
          }
          return result;
        }

        @Override
        public Optional<O> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
          throw new RuntimeException("Not support");
        }
      };
    } else {
      throw new RuntimeException("Channel is null");
    }
  }


  /*
  private boolean isFlushTime() {
    return byteBufOutputStream.buffer().readableBytes() >= Constants.FLUSH_BYTES;
  }
  */

  /*
  @Override
  public void flush() {
    try {
      if (channel != null) {
        byteBufOutputStream.close();
        channel.write(new OffloadingEvent(OffloadingEvent.Type.DATA, byteBufOutputStream.buffer()));
        byteBufOutputStream = new ByteBufOutputStream(channel.alloc().ioBuffer());
        byteBufOutputStream.writeInt(OffloadingEvent.Type.DATA.ordinal());
        encoder = inputEncoderFactory.create(byteBufOutputStream);
      }
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
*/

  @Override
  public void finishOffloading() {
    /*
    try {
      byteBufOutputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    */

    if (channel != null) {
      //byteBufOutputStream.buffer().release();
      channel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.END, new byte[0], 0));
    } else {
      throw new RuntimeException("Channel cannot be null when finishOffloading");
      /*
      // waiting for channel
      while (channel == null) {
        try {
          Thread.sleep(300);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      // send buffered data
      channel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.DATA, byteBufOutputStream.buffer()));
      channel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.END, new byte[0], 0));
      */
    }

    offloadingWorkerFactory.deleteOffloadingWorker(this);
    channelThread.shutdown();
    finished = true;
  }

  @Override
  public String toString() {
    return workerId;
  }
}
