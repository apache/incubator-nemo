package org.apache.nemo.runtime.executor.offloading;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.nemo.offloading.client.SharedCachedPool;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.offloading.common.TaskHandlingEvent;
import org.apache.nemo.runtime.message.comm.ControlMessage;
import org.apache.nemo.runtime.executor.common.ExecutorMetrics;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.runtime.executor.common.datatransfer.DataFrameEncoder;
import org.apache.nemo.runtime.message.PersistentConnectionToMasterMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.nemo.offloading.common.OffloadingExecutorControlEvent.Type.ACTIVATE;
import static org.apache.nemo.offloading.common.OffloadingExecutorControlEvent.Type.DEACTIVATE;
import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.LAMBDA_OFFLOADING_REQUEST_ID;

public final class StreamingLambdaWorkerProxy<I, O> implements OffloadingWorker<I, O> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingLambdaWorkerProxy.class.getName());
  private volatile Channel dataChannel;
  //private volatile int dataProcessingCnt = 0;
  private volatile boolean finished = false;
  private final BlockingQueue<OffloadingMasterEvent> endQueue;

  // two data stream: when channel is null, we buffer byte in byte array output
  // otherwise, we use byteBufOutputStream directly.
  //private ObjectOutputStream objectOutputStream;
  //private ByteBufOutputStream byteBufOutputStream;

  final ExecutorService channelThread = SharedCachedPool.POOL;

  private final ExecutorService closeThread = SharedCachedPool.POOL;

  private final DescriptiveStatistics cpuAverage;
  private final DescriptiveStatistics processingRateAverage;

  private ExecutorMetrics executorMetrics;

  private final Set<String> readyTasks = new HashSet<>();

  private int workerRequestId;
  private final PersistentConnectionToMasterMap toMasterMap;

  private volatile boolean activated = false;

  public StreamingLambdaWorkerProxy(final PersistentConnectionToMasterMap toMasterMap,
                                    final Future<Pair<Integer, Channel>> channelFuture,
                                    final Map<Channel, EventHandler<OffloadingExecutorControlEvent>> channelEventHandlerMap,
                                    final EventHandler<Pair<OffloadingWorker, OffloadingExecutorControlEvent>> eventHandler) {
    this.toMasterMap = toMasterMap;
    this.cpuAverage = new DescriptiveStatistics();
    this.processingRateAverage = new DescriptiveStatistics();
    this.processingRateAverage.setWindowSize(3);
    cpuAverage.setWindowSize(3);

    this.endQueue = new LinkedBlockingQueue<>();

    channelThread.execute(() -> {
      try {
        final Pair<Integer, Channel> pair = channelFuture.get();
        workerRequestId = pair.left();
        dataChannel = pair.right();
        channelEventHandlerMap.put(dataChannel, new EventHandler<OffloadingExecutorControlEvent>() {
            @Override
            public void onNext(OffloadingExecutorControlEvent msg) {
              if (msg.getType().equals(ACTIVATE)) {
                LOG.info("Activated worker {} in executor", workerRequestId);
                activated = true;
              } else if (msg.getType().equals(DEACTIVATE)) {
                LOG.info("Deactivated worker {} in executor", workerRequestId);
                activated = false;
                deactivateInvoked.set(false);
              } else {
                eventHandler.onNext(Pair.of(StreamingLambdaWorkerProxy.this, msg));
              }
            }
          });
        LOG.info("Get worker request id {}, data channel {}", workerRequestId, dataChannel);

      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });
  }

  private final AtomicBoolean deactivateInvoked = new AtomicBoolean(false);

  @Override
  public void deactivate() {
    if (deactivateInvoked.compareAndSet(false, true) && activated) {
      LOG.info("Deactivating worker {} in executor", workerRequestId);
      final TaskControlMessage msg = new TaskControlMessage(TaskControlMessage.TaskControlMessageType.DEACTIVATE_LAMBDA,
        0, 0, "1", null);
      dataChannel.writeAndFlush(msg);
    }
  }

  private long prevFlushBufferTrackTime;
  private AtomicLong offloadData = new AtomicLong(0);
  private AtomicLong rateControlData = new AtomicLong(0);

  private long desirableRate() {
    final long cnt = offloadData.get();
    if (cnt < 3000) {
      return 3000;
    } else if (cnt < 6000) {
      return 6000;
    } else if (cnt < 10000) {
      return 10000;
    } else {
      return 1000000;
    }
  }

  private boolean rateControl() {
    final long curr = System.currentTimeMillis();
    if (curr - prevFlushBufferTrackTime >= 5) {
      final long elapsed = curr - prevFlushBufferTrackTime;
      final long cnt = rateControlData.get();
      if (cnt * (1000 / (double)elapsed) > desirableRate()) {
        // Throttle !!
        return false;
      } else {
        prevFlushBufferTrackTime = curr;
        rateControlData.addAndGet(-cnt);
      }
    }
    return true;
  }

  @Override
  public boolean isInputAccepted(String taskId) {
    if (!isReady()) {
      return false;
    }
    return rateControl();
  }

  @Override
  public boolean isActivated() {
    return activated;
  }

  @Override
  public boolean isReady() {
    return dataChannel != null;
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public Channel getChannel() {
    return dataChannel;
  }

  @Override
  public double getLoad() {
    return cpuAverage.getMean();
  }

  @Override
  public String getId() {
    return Integer.toString(workerRequestId);
  }

  @Override
  public int getRequestId() {
    return workerRequestId;
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
  public void writeControl(ControlMessage.Message message) {
    if (message.getType().equals(ControlMessage.MessageType.TaskSendToLambda)) {
      prevFlushBufferTrackTime = System.currentTimeMillis();
    }

    toMasterMap.getMessageSender(LAMBDA_OFFLOADING_REQUEST_ID)
      .send(message);
  }

  private final AtomicLong byteSent = new AtomicLong(0);

  @Override
  public long getByteSent() {
    return byteSent.get();
  }

  @Override
  public void writeData(final int pipeIndex, final TaskHandlingEvent event) {
    if (event.isControlMessage()) {
      dataChannel.writeAndFlush(event);
    } else {
      offloadData.getAndIncrement();
      rateControlData.getAndIncrement();
      final ByteBuf byteBuf = event.getDataByteBuf();
      final Object finalData = DataFrameEncoder.DataFrame.newInstance(
        pipeIndex, byteBuf, byteBuf.readableBytes(), true);

      byteSent.addAndGet(byteBuf.readableBytes() + 2 + Integer.BYTES * 2);

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

  @Override
  public long getNumOffloadedData() {
    return offloadData.get();
  }


  @Deprecated
  private synchronized void forceClose() {
    /*
    if (controlChannel != null) {
      //byteBufOutputStream.buffer().release();
      closeThread.execute(() -> {
        LOG.info("ForceClose: Send end mesesage to worker {}", workerId);
        controlChannel.writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.END, new byte[0], 0));

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
        controlChannel.writeAndFlush(new OffloadingMasterEvent(OffloadingMasterEvent.Type.END, new byte[0], 0));

      });
    }

    finished = true;
    // offloadingWorkerFactory.deleteOffloadingWorker(this);
    */
  }


  @Override
  public String toString() {
    return Integer.toString(workerRequestId) + "/" + executorMetrics;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StreamingLambdaWorkerProxy<?, ?> that = (StreamingLambdaWorkerProxy<?, ?>) o;
    return workerRequestId == that.workerRequestId &&
      Objects.equals(dataChannel, that.dataChannel);
  }

  @Override
  public int hashCode() {

    return Objects.hash(dataChannel, workerRequestId);
  }
}
