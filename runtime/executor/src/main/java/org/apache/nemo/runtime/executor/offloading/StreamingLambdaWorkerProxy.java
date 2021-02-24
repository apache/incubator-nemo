package org.apache.nemo.runtime.executor.offloading;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.nemo.offloading.client.SharedCachedPool;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.executor.common.ExecutorMetrics;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.datatransfer.DataFrameEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class StreamingLambdaWorkerProxy<I, O> implements OffloadingWorker<I, O> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingLambdaWorkerProxy.class.getName());
  private volatile Channel controlChannel;
  private volatile Channel dataChannel;
  //private volatile int dataProcessingCnt = 0;
  private volatile boolean finished = false;
  private final BlockingQueue<OffloadingEvent> endQueue;
  private final ConcurrentMap<Channel, EventHandler<OffloadingEvent>> channelEventHandlerMap;
  private final Future<Pair<Channel, Pair<Channel, OffloadingEvent>>> channelFuture;

  private final AtomicInteger numTasks = new AtomicInteger(0);

  // two data stream: when channel is null, we buffer byte in byte array output
  // otherwise, we use byteBufOutputStream directly.
  //private ObjectOutputStream objectOutputStream;
  //private ByteBufOutputStream byteBufOutputStream;

  final ExecutorService channelThread = SharedCachedPool.POOL;

  private final String workerId;

  private final ExecutorService closeThread = SharedCachedPool.POOL;

  private final DescriptiveStatistics cpuAverage;
  private final DescriptiveStatistics processingRateAverage;

  private ExecutorMetrics executorMetrics;

  private final Set<String> readyTasks = new HashSet<>();

  public StreamingLambdaWorkerProxy(final int workerId,
                                    final Future<Pair<Channel, Pair<Channel, OffloadingEvent>>> channelFuture,
                                    final OffloadingWorkerFactory offloadingWorkerFactory,
                                    final ConcurrentMap<Channel, EventHandler<OffloadingEvent>> channelEventHandlerMap,
                                    final OffloadingEncoder offloadingEncoder,
                                    final OffloadingDecoder<O> outputDecoder,
                                    final EventHandler eventHandler) {
    this.workerId = Integer.toString(workerId);
    this.channelFuture = channelFuture;
    this.cpuAverage = new DescriptiveStatistics();
    this.processingRateAverage = new DescriptiveStatistics();
    this.processingRateAverage.setWindowSize(3);
    cpuAverage.setWindowSize(3);

    this.channelEventHandlerMap = channelEventHandlerMap;
    this.endQueue = new LinkedBlockingQueue<>();

    channelThread.execute(() -> {
      try {
        final Pair<Channel, Pair<Channel, OffloadingEvent>> pair = channelFuture.get();
        controlChannel = pair.left();
        dataChannel = pair.right().left();

        LOG.info("Get control channel {}, data channel {}", controlChannel, dataChannel);

        /*
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
        */

        channelEventHandlerMap.put(controlChannel, new EventHandler<OffloadingEvent>() {
          @Override
          public void onNext(OffloadingEvent msg) {
            switch (msg.getType()) {
              case RESULT: {
                final ByteBufInputStream bis = new ByteBufInputStream(msg.getByteBuf());
                try {
                  final int hasInstance = bis.readByte();
                  if (hasInstance != 0) {
                    final long st = System.currentTimeMillis();
                    final O data = outputDecoder.decode(msg.getByteBuf());
                    final long et = System.currentTimeMillis();

                    eventHandler.onNext(data);
                  }
                  msg.getByteBuf().release();
                } catch (IOException e) {
                  e.printStackTrace();
                  throw new RuntimeException();
                }
                break;
              }
              case EXECUTOR_METRICS:
              case TASK_FINISH_DONE:
              case TASK_READY: {
                eventHandler.onNext(Pair.of(StreamingLambdaWorkerProxy.this, msg));
                break;
              }
              case CPU_LOAD: {
                final double load = msg.getByteBuf().readDouble();
                LOG.info("Receive cpu load {}", load);
                cpuAverage.addValue(load);
                break;
              }
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
    return controlChannel != null;
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public Channel getChannel() {
    return controlChannel;
  }

  @Override
  public double getLoad() {
    return cpuAverage.getMean();
  }

  @Override
  public String getId() {
    return workerId;
  }

  @Override
  public void addReadyTask(String taskId) {
    synchronized (readyTasks) {
      readyTasks.add(taskId);
    }
  }

  @Override
  public void removeDoneTask(String taskId) {
    synchronized (readyTasks) {
      readyTasks.remove(taskId);
    }
  }

  @Override
  public boolean hasReadyTask(String taskId) {
    synchronized (readyTasks) {
      return readyTasks.contains(taskId);
    }
  }

  @Override
  public void writeControl(OffloadingEvent offloadingEvent) {
    controlChannel.writeAndFlush(offloadingEvent);
  }

  @Override
  public void writeData(final int pipeIndex, final TaskHandlingEvent event) {
    if (event.isControlMessage()) {
      dataChannel.writeAndFlush(event);
    } else {
      final ByteBuf byteBuf = event.getDataByteBuf();
      final Object finalData = DataFrameEncoder.DataFrame.newInstance(
        pipeIndex, byteBuf, byteBuf.readableBytes(), true);
      dataChannel.write(finalData);
    }
  }

  @Override
  public void writeSourceData(final int index,
                              final Serializer serializer,
                              final Object event) {
    final ByteBuf byteBuf = dataChannel.alloc().ioBuffer();
    final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);

    try {
      serializer.getEncoderFactory().create(bos).encode(event);
      bos.close();

      final Object finalData = DataFrameEncoder.DataFrame.newInstance(
        index, byteBuf, byteBuf.readableBytes(), true);
      dataChannel.write(finalData);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setMetric(ExecutorMetrics executorMetrics) {
    this.executorMetrics = executorMetrics;
    processingRateAverage.addValue(executorMetrics.processingRate);
  }

  @Override
  public double getProcessingRate() {
    return processingRateAverage.getMean();
  }

  @Override
  public Optional<ExecutorMetrics> getExecutorMetrics() {
    if (executorMetrics == null) {
      return Optional.empty();
    } else {
      return Optional.of(executorMetrics);
    }
  }


  @Deprecated
  private synchronized void forceClose() {
    if (controlChannel != null) {
      //byteBufOutputStream.buffer().release();
      closeThread.execute(() -> {
        LOG.info("ForceClose: Send end mesesage to worker {}", workerId);
        controlChannel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.END, new byte[0], 0));

      });
    } else {
      closeThread.execute(() -> {
        while (controlChannel == null) {
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

          LOG.info("ForceClose: Send end mesesage to worker {}", workerId);
        controlChannel.writeAndFlush(new OffloadingEvent(OffloadingEvent.Type.END, new byte[0], 0));

      });
    }

    finished = true;
    // offloadingWorkerFactory.deleteOffloadingWorker(this);
  }


  @Override
  public String toString() {
    return workerId + "/" + executorMetrics;
  }
}
