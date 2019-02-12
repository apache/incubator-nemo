package org.apache.nemo.offloading.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.NemoEvent;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.concurrent.*;

public final class LambdaWorkerProxy<I, O> implements OffloadingWorker<I, O> {
  private static final Logger LOG = LoggerFactory.getLogger(LambdaWorkerProxy.class.getName());
  private volatile Channel channel;
  private volatile boolean finished = false;
  private final BlockingQueue<NemoEvent> endQueue;
  private final ConcurrentMap<Channel, EventHandler<NemoEvent>> channelEventHandlerMap;

  //private final EncoderFactory inputEncoderFactory;
  //private final DecoderFactory outputDecoderFactory;


  //private EncoderFactory.Encoder encoder;

  private final OffloadingWorkerFactory offloadingWorkerFactory;

  private final Future<Channel> channelFuture;

  private int dataIdCnt = 0;

  // two data stream: when channel is null, we buffer byte in byte array output
  // otherwise, we use byteBufOutputStream directly.
  //private ObjectOutputStream objectOutputStream;
  //private ByteBufOutputStream byteBufOutputStream;

  final ExecutorService channelThread = Executors.newSingleThreadExecutor();

  private final ConcurrentMap<Integer, O> resultMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<Integer, Boolean> pendingData = new ConcurrentHashMap<>();


  public LambdaWorkerProxy(final Future<Channel> channelFuture,
                           final OffloadingWorkerFactory offloadingWorkerFactory,
                           final ConcurrentMap<Channel, EventHandler<NemoEvent>> channelEventHandlerMap,
                           final EncoderFactory inputEncoderFactory,
                           final DecoderFactory outputDecoderFactory) {
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
      byteBufOutputStream.writeInt(NemoEvent.Type.DATA.ordinal());
      this.encoder = inputEncoderFactory.create(byteBufOutputStream);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    */

    channelThread.execute(() -> {
      try {
        channel = channelFuture.get();
        channelEventHandlerMap.put(channel, new EventHandler<NemoEvent>() {
          @Override
          public void onNext(NemoEvent msg) {
            switch (msg.getType()) {
              case RESULT:
                LOG.info("Receive result");
                final ByteBufInputStream bis = new ByteBufInputStream(msg.getByteBuf());
                try {
                  final DecoderFactory.Decoder<O> decoder = outputDecoderFactory.create(bis);
                  final O data = decoder.decode();
                  final int resultId = bis.readInt();
                  resultMap.put(resultId, data);
                  pendingData.remove(resultId);
                  msg.getByteBuf().release();
                } catch (IOException e) {
                  e.printStackTrace();
                  throw new RuntimeException();
                }
                break;
              case END:
                LOG.info("Receive end");
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
  public Future<O> execute(final ByteBuf input) {
    final int dataId = dataIdCnt++;
    input.writeInt(dataId);

    pendingData.put(dataId, true);

    if (channel != null) {
      LOG.info("Write data");
      channel.writeAndFlush(new NemoEvent(NemoEvent.Type.DATA, input));
      return new Future<O>() {
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
          return resultMap.get(dataId) != null;
        }

        @Override
        public O get() throws InterruptedException, ExecutionException {
          while (resultMap.get(dataId) == null) {
           Thread.sleep(200);
          }
          return resultMap.remove(dataId);
        }

        @Override
        public O get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
          return null;
        }
      };
    } else {
      throw new RuntimeException("Channel is null");
    }
  }

  @Override
  public Future<O> execute(I input) {
    throw new RuntimeException("Not supported");
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
        channel.write(new NemoEvent(NemoEvent.Type.DATA, byteBufOutputStream.buffer()));
        byteBufOutputStream = new ByteBufOutputStream(channel.alloc().ioBuffer());
        byteBufOutputStream.writeInt(NemoEvent.Type.DATA.ordinal());
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
      channel.writeAndFlush(new NemoEvent(NemoEvent.Type.END, new byte[0], 0));
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
      channel.writeAndFlush(new NemoEvent(NemoEvent.Type.DATA, byteBufOutputStream.buffer()));
      channel.writeAndFlush(new NemoEvent(NemoEvent.Type.END, new byte[0], 0));
      */
    }

    offloadingWorkerFactory.deleteOffloadingWorker(this);
    channelThread.shutdown();
    finished = true;
  }
}
