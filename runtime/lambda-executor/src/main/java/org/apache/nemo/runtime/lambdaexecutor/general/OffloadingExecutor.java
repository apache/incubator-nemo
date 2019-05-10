package org.apache.nemo.runtime.lambdaexecutor.general;

import io.netty.channel.socket.SocketChannel;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.offloading.common.OffloadingOutputCollector;
import org.apache.nemo.offloading.common.OffloadingTransform;
import org.apache.nemo.runtime.executor.common.ExecutorThread;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.executor.common.datatransfer.AckScheduledService;
import org.apache.nemo.runtime.executor.common.datatransfer.ControlFrameEncoder;
import org.apache.nemo.runtime.executor.common.datatransfer.DataFrameEncoder;
import org.apache.nemo.runtime.executor.common.datatransfer.VMScalingClientTransport;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingHeartbeatEvent;
import org.apache.nemo.runtime.lambdaexecutor.datatransfer.*;
import org.apache.nemo.runtime.lambdaexecutor.downstream.TaskEndEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class OffloadingExecutor implements OffloadingTransform<Object, Object> {

  private static final Logger LOG = LoggerFactory.getLogger(OffloadingExecutor.class.getName());

  private final int executorThreadNum = 1;
  private List<ExecutorThread> executorThreads;
  private ScheduledExecutorService scheduledExecutorService;

  private final AtomicInteger receivedTasks = new AtomicInteger(0);

  private final ConcurrentMap<SocketChannel, Boolean> channels;
  private final String executorId;

  private final Map<String, InetSocketAddress> executorAddressMap;
  private final Map<String, Serializer> serializerMap;
  private final Map<Pair<RuntimeEdge, Integer>, String> taskExecutorIdMap;


  private transient LambdaByteTransport byteTransport;
  private transient IntermediateDataIOFactory intermediateDataIOFactory;
  private transient PipeManagerWorker pipeManagerWorker;
  private transient OffloadingOutputCollector oc;
  private transient VMScalingClientTransport clientTransport;
  private transient AckScheduledService ackScheduledService;

  private final boolean isInVm;
  private final ConcurrentMap<TaskExecutor, ExecutorThread> taskAssignedMap;

  public OffloadingExecutor(final Map<String, InetSocketAddress> executorAddressMap,
                            final Map<String, Serializer> serializerMap,
                            final Map<Pair<RuntimeEdge, Integer>, String> taskExecutorIdMap,
                            final boolean isInVm) {
    this.channels = new ConcurrentHashMap<>();
    this.executorId = "lambdaExecutor";
    this.executorAddressMap = executorAddressMap;
    this.serializerMap = serializerMap;
    this.taskExecutorIdMap = taskExecutorIdMap;
    this.isInVm = isInVm;
    this.taskAssignedMap = new ConcurrentHashMap<>();
  }
  /**
   * Extracts task index from a task ID.
   *
   * @param taskId the task ID to extract.
   * @return the index.
   */
  public static int getIndexFromTaskId(final String taskId) {
    return Integer.valueOf(taskId.split("-")[1]);
  }

  @Override
  public void prepare(OffloadingContext context, OffloadingOutputCollector outputCollector) {
    this.oc = outputCollector;
    this.ackScheduledService = new AckScheduledService();

    executorThreads = new ArrayList<>();
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    for (int i = 0; i < executorThreadNum; i++) {
      executorThreads.add(
        new ExecutorThread(scheduledExecutorService, 1, "lambda"));
      executorThreads.get(i).start();
    }

    final NativeChannelImplementationSelector selector = new NativeChannelImplementationSelector();
    final ControlFrameEncoder controlFrameEncoder = new ControlFrameEncoder();
    final DataFrameEncoder dataFrameEncoder = new DataFrameEncoder();

    this.clientTransport = new VMScalingClientTransport();

    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.scheduleAtFixedRate(() -> {
      LOG.info("Flush {} channels: {}", channels.size(), channels.keySet());

      for (final TaskExecutor taskExecutor : taskAssignedMap.keySet()) {
        final long et = taskExecutor.getTaskExecutionTime().get();
        taskExecutor.getTaskExecutionTime().getAndAdd(-et);
        LOG.info("Send heartbeat {}/{}", taskExecutor.getId(), et);
        outputCollector.emit(new OffloadingHeartbeatEvent(
          taskExecutor.getId(), getIndexFromTaskId(taskExecutor.getId()), et));
      }

      for (final SocketChannel channel : channels.keySet()) {
        if (channel.isOpen()) {
          channel.flush();
        }
      }
    }, 1, 1, TimeUnit.SECONDS);

    final LambdaByteTransportChannelInitializer initializer =
      new LambdaByteTransportChannelInitializer(byteTransport.getChannelGroup(),
        controlFrameEncoder, dataFrameEncoder, channels, executorId, clientTransport, ackScheduledService);

    byteTransport = new LambdaByteTransport(
      executorId, selector, initializer, executorAddressMap);

    final ByteTransfer byteTransfer = new ByteTransfer(byteTransport, executorId);

    pipeManagerWorker =
      new PipeManagerWorker(executorId, byteTransfer, taskExecutorIdMap, serializerMap);

    intermediateDataIOFactory =
      new IntermediateDataIOFactory(pipeManagerWorker);
  }

  @Override
  public void onData(Object event) {

    if (event instanceof OffloadingTask) {
      // TODO: receive tasks
      final int executorIndex = receivedTasks.getAndIncrement() % executorThreadNum;
      final ExecutorThread executorThread = executorThreads.get(executorIndex);

      final OffloadingTask task = (OffloadingTask) event;

      final OffloadingTaskExecutor taskExecutor = new OffloadingTaskExecutor(
        task,
        executorAddressMap,
        serializerMap,
        taskExecutorIdMap,
        byteTransport,
        pipeManagerWorker,
        intermediateDataIOFactory,
        oc,
        scheduledExecutorService);

      executorThread.addNewTask(taskExecutor);

      taskAssignedMap.put(taskExecutor, executorThread);

    } else if (event instanceof TaskEndEvent) {
      // TODO
      final TaskEndEvent endEvent = (TaskEndEvent) event;
      final TaskExecutor deletedTask = findTask(endEvent.taskId);
      final ExecutorThread executorThread = taskAssignedMap.remove(deletedTask);
      executorThread.deleteTask(deletedTask);

    } else {
      throw new RuntimeException("Unsupported event type: " + event);
    }
  }

  private TaskExecutor findTask(final String taskId) {
    for (final TaskExecutor taskExecutor : taskAssignedMap.keySet()) {
      if (taskExecutor.getId().equals(taskId)) {
        return taskExecutor;
      }
    }

    throw new RuntimeException("Cannot find task " + taskId);
  }

  @Override
  public void close() {

  }
}
