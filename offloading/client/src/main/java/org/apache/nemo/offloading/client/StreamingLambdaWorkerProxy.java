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

public final class StreamingLambdaWorkerProxy<I, O> implements OffloadingWorker<I, O> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingLambdaWorkerProxy.class.getName());
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

  private final String workerId;

  private final EventHandler<O> eventHandler;

  private final ExecutorService closeThread = Executors.newSingleThreadExecutor();

  public StreamingLambdaWorkerProxy(final int workerId,
                                    final Future<Pair<Channel, OffloadingEvent>> channelFuture,
                                    final OffloadingWorkerFactory offloadingWorkerFactory,
                                    final ConcurrentMap<Channel, EventHandler<OffloadingEvent>> channelEventHandlerMap,
                                    final OffloadingEncoder offloadingEncoder,
                                    final OffloadingDecoder<O> outputDecoder,
                                    final EventHandler<O> eventHandler) {
    this.workerId = Integer.toString(workerId);
    this.channelFuture = channelFuture;
    this.offloadingWorkerFactory = offloadingWorkerFactory;
    this.eventHandler = eventHandler;

    this.channelEventHandlerMap = channelEventHandlerMap;
    this.endQueue = new LinkedBlockingQueue<>();

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
                  if (hasInstance != 0) {
                    final O data = outputDecoder.decode(bis);
                    eventHandler.onNext(data);
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

  @Override
  public boolean isReady() {
    return channel != null;
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
    return null;
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
    input.writeInt(dataId);

    if (channel != null) {
      if (Constants.enableLambdaLogging) {
        LOG.info("Write data from worker {}, id: {}", workerId, dataId);
      }

      channel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.DATA, input));
    } else {
      throw new RuntimeException("Channel is null");
    }

    return null;
  }


  @Override
  public synchronized void forceClose() {
    if (channel != null) {
      //byteBufOutputStream.buffer().release();
      closeThread.execute(() -> {
        LOG.info("ForceClose: Send end mesesage to worker {}", workerId);
        channel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.END, new byte[0], 0));

      });
    } else {
      closeThread.execute(() -> {
        while (channel == null) {
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

          LOG.info("ForceClose: Send end mesesage to worker {}", workerId);
          channel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.END, new byte[0], 0));

      });
    }

    finished = true;
  }

  @Override
  public synchronized <T> T finishOffloading() {

    if (channel != null) {
      //byteBufOutputStream.buffer().release();
      channel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.END, new byte[0], 0));
    } else {
      throw new RuntimeException("Channel cannot be null when finishOffloading");
    }

    //offloadingWorkerFactory.deleteOffloadingWorker(this);
    //channelThread.shutdown();
    finished = true;

    return null;
  }

  @Override
  public String toString() {
    return workerId;
  }
}
