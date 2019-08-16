package org.apache.nemo.runtime.lambdaexecutor.general;

import com.sun.management.OperatingSystemMXBean;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.nemo.common.*;
import org.apache.nemo.offloading.common.OffloadingOutputCollector;
import org.apache.nemo.offloading.common.OffloadingTransform;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.nemo.runtime.lambdaexecutor.ReadyTask;
import org.apache.nemo.runtime.lambdaexecutor.datatransfer.RelayServerClient;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingHeartbeatEvent;
import org.apache.nemo.runtime.lambdaexecutor.datatransfer.*;
import org.apache.nemo.runtime.lambdaexecutor.downstream.TaskEndEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class OffloadingExecutor implements OffloadingTransform<Object, Object> {

  private static final Logger LOG = LoggerFactory.getLogger(OffloadingExecutor.class.getName());

  private final int executorThreadNum;
  private List<ExecutorThread> executorThreads;
  private ScheduledExecutorService scheduledExecutorService;

  private final AtomicInteger receivedTasks = new AtomicInteger(0);

  private final ConcurrentMap<SocketChannel, Boolean> channels;
  private final String executorId;

  private final Map<String, InetSocketAddress> executorAddressMap;
  private final Map<String, Serializer> serializerMap;
  private final Map<NemoTriple<String, Integer, Boolean>, String> taskExecutorIdMap;


  private transient LambdaByteTransport byteTransport;
  private transient IntermediateDataIOFactory intermediateDataIOFactory;
  private transient PipeManagerWorker pipeManagerWorker;
  private transient OffloadingOutputCollector oc;
  private transient VMScalingClientTransport clientTransport;
  private transient AckScheduledService ackScheduledService;

  private final ConcurrentMap<TaskExecutor, ExecutorThread> taskAssignedMap;
  private final ConcurrentMap<TaskExecutor, Long> taskExecutorStartTimeMap;
  private final Map<TransferKey, Integer> taskTransferIndexMap;

  private final String relayServerAddress;
  private final int relayServerPort;
  private final int rendevousServerPort;

  private transient RelayServerClient relayServerClient;
  private transient RendevousServerClient rendevousServerClient;
  private final ConcurrentMap<String, TaskLoc> taskLocMap;

  private transient ExecutorService prepareService;

  private transient ExecutorGlobalInstances executorGlobalInstances;

  private transient OutputWriterFlusher outputWriterFlusher;

  private final Map<String, Pair<String, Integer>> relayServerInfo;

  private final List<OffloadingTask> pendingTask;
  private final List<ReadyTask> readyTasks;

  private final String rendevousServerAddress;

  private transient ExecutorService executorStartService;

  public OffloadingExecutor(final int executorThreadNum,
                            final Map<String, InetSocketAddress> executorAddressMap,
                            final Map<String, Serializer> serializerMap,
                            final Map<NemoTriple<String, Integer, Boolean>, String> taskExecutorIdMap,
                            final Map<TransferKey, Integer> taskTransferIndexMap,
                            final String relayServerAddress,
                            final int relayServerPort,
                            final String rendevousServerAddress,
                            final int rendevousServerPort,
                            final String executorId,
                            final Map<String, Pair<String, Integer>> relayServerInfo) {
    this.executorThreadNum = executorThreadNum;
    this.channels = new ConcurrentHashMap<>();
    this.executorId = executorId;
    this.executorAddressMap = executorAddressMap;
    this.serializerMap = serializerMap;
    this.taskExecutorIdMap = taskExecutorIdMap;
    this.taskTransferIndexMap = taskTransferIndexMap;
    this.taskAssignedMap = new ConcurrentHashMap<>();
    this.taskExecutorStartTimeMap = new ConcurrentHashMap<>();
    this.relayServerAddress = relayServerAddress;
    this.relayServerPort = relayServerPort;
    this.rendevousServerAddress = rendevousServerAddress;
    this.rendevousServerPort = rendevousServerPort;
    this.taskLocMap = new ConcurrentHashMap<>();
    this.relayServerInfo = relayServerInfo;
    this.pendingTask = new ArrayList<>();
    this.readyTasks = new ArrayList<>();
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

    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    /*
    scheduledExecutorService.scheduleAtFixedRate(() -> {
      final OperatingSystemMXBean bean =
        (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

      final double cpuLoad = bean.getProcessCpuLoad();

      if (cpuLoad > 0.78) {
        if (!Throttled.getInstance().getThrottled()) {
          System.out.println("throttle true");
          Throttled.getInstance().setThrottle(true);
        }
      }

      if (cpuLoad < 0.65) {
        if (Throttled.getInstance().getThrottled()) {
          System.out.println("throttle false");
          Throttled.getInstance().setThrottle(false);
        }
      }
    }, 1, 1, TimeUnit.SECONDS);
    */



    LOG.info("ExecutorAddressMap: {}", executorAddressMap);

    this.executorStartService = Executors.newCachedThreadPool();

    this.oc = outputCollector;
    this.ackScheduledService = new AckScheduledService();
    this.prepareService = Executors.newCachedThreadPool();
    this.executorGlobalInstances = new ExecutorGlobalInstances();
    this.outputWriterFlusher = new OutputWriterFlusher(200);

    executorThreads = new ArrayList<>();
    for (int i = 0; i < executorThreadNum; i++) {
      executorThreads.add(
        new ExecutorThread(1, "lambda"));
      executorThreads.get(i).start();
    }

    final NativeChannelImplementationSelector selector = new NativeChannelImplementationSelector();
    final ControlFrameEncoder controlFrameEncoder = new ControlFrameEncoder();
    final DataFrameEncoder dataFrameEncoder = new DataFrameEncoder();

    this.clientTransport = new VMScalingClientTransport();

    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.scheduleAtFixedRate(() -> {
      LOG.info("Flush {} channels: {}", channels.size(), channels.keySet());

      final long currTime = System.currentTimeMillis();

      for (final TaskExecutor taskExecutor : taskAssignedMap.keySet()) {
        final long et = taskExecutor.getTaskExecutionTime().get();
        taskExecutor.getTaskExecutionTime().getAndAdd(-et);

        if (currTime - taskExecutorStartTimeMap.get(taskExecutor)
          >= TimeUnit.SECONDS.toMillis(10)) {
          LOG.info("Send heartbeat {}/{}", taskExecutor.getId(), et);
          outputCollector.emit(new OffloadingHeartbeatEvent(
            taskExecutor.getId(), getIndexFromTaskId(taskExecutor.getId()), et));
        }
      }

      for (final SocketChannel channel : channels.keySet()) {
        if (channel.isOpen()) {
          channel.flush();
        }
      }
    }, 1, 1, TimeUnit.SECONDS);

    final ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    final ConcurrentMap<Integer, ByteInputContext> inputContexts = new ConcurrentHashMap<>();
    final ConcurrentMap<Integer, ByteOutputContext> outputContexts = new ConcurrentHashMap<>();

    final LambdaByteTransportChannelInitializer initializer =
      new LambdaByteTransportChannelInitializer(channelGroup,
        controlFrameEncoder, dataFrameEncoder, channels, executorId, ackScheduledService,
        taskTransferIndexMap, inputContexts, outputContexts, outputWriterFlusher);

    // Relay server channel initializer
    final RelayServerClientChannelInitializer relayServerClientChannelInitializer =
      new RelayServerClientChannelInitializer(channelGroup,
        controlFrameEncoder, dataFrameEncoder, channels, executorId, ackScheduledService,
        taskTransferIndexMap, inputContexts, outputContexts, outputWriterFlusher);

    final EventLoopGroup clientGroup = new NioEventLoopGroup(2, new DefaultThreadFactory("relayClient"));
    final Bootstrap clientBootstrap = new Bootstrap()
      .group(clientGroup)
      .channel(NioSocketChannel.class)
      .handler(relayServerClientChannelInitializer)
      .option(ChannelOption.SO_REUSEADDR, true);


    this.rendevousServerClient = new RendevousServerClient(rendevousServerAddress, rendevousServerPort);

    this.relayServerClient = new RelayServerClient(
      clientGroup, clientBootstrap, relayServerAddress, relayServerPort, relayServerInfo, rendevousServerClient);

    initializer.setRelayServerClient(relayServerClient);
    relayServerClientChannelInitializer.setRelayServerClient(relayServerClient);

    byteTransport = new LambdaByteTransport(
      executorId, selector, initializer, executorAddressMap, channelGroup, relayServerAddress, relayServerPort);

    final ByteTransfer byteTransfer = new ByteTransfer(byteTransport, executorId);

    initializer.setByteTransfer(byteTransfer);
    relayServerClientChannelInitializer.setByteTransfer(byteTransfer);

    pipeManagerWorker =
      new PipeManagerWorker(executorId, byteTransfer, taskExecutorIdMap, serializerMap, taskLocMap, relayServerClient);

    intermediateDataIOFactory =
      new IntermediateDataIOFactory(pipeManagerWorker);
  }

  private OffloadingTask findAndRemoveOffloadingTask(final String taskId) {
    synchronized (pendingTask) {
      final Iterator<OffloadingTask> iterator = pendingTask.iterator();
      while (iterator.hasNext()) {
        final OffloadingTask task = iterator.next();
        if (task.taskId.equals(taskId)) {
          iterator.remove();
          return task;
        }
      }
    }
    return null;
  }

  private ReadyTask findAndRemoveReadyTask(final String taskId) {
    synchronized (readyTasks) {
      final Iterator<ReadyTask> iterator = readyTasks.iterator();
      while (iterator.hasNext()) {
        final ReadyTask task = iterator.next();
        if (task.taskId.equals(taskId)) {
          iterator.remove();
          return task;
        }
      }
    }
    return null;
  }

  private void startTask(final ReadyTask readyTask,
                         final OffloadingTask task) {

    LOG.info("Start task {}", task.taskId);

    final int executorIndex = receivedTasks.getAndIncrement() % executorThreadNum;
    final ExecutorThread executorThread = executorThreads.get(executorIndex);

    LOG.info("Receive task {}, assign it to executor-{}", task.taskId, executorIndex);


    for (final Map.Entry<String, TaskLoc> entry
      : readyTask.taskLocationMap.entrySet()) {
      taskLocMap.put(entry.getKey(), entry.getValue());
    }

    if (RuntimeIdManager.getStageIdFromTaskId(task.taskId).equals("Stage1")) {
      LOG.info("TaskLocMap of {}: {}", task.taskId, taskLocMap);
    }

    executorStartService.execute(() -> {

      final OffloadingTaskExecutor taskExecutor = new OffloadingTaskExecutor(
        task,
        executorAddressMap,
        serializerMap,
        byteTransport,
        pipeManagerWorker,
        intermediateDataIOFactory,
        oc,
        scheduledExecutorService,
        prepareService,
        executorGlobalInstances);

      LOG.info("End of creating offloading task {}", task.taskId);

      executorThread.addNewTask(taskExecutor);

      taskExecutorStartTimeMap.put(taskExecutor, System.currentTimeMillis());
      taskAssignedMap.put(taskExecutor, executorThread);

      // Emit offloading done
      synchronized (oc) {
        oc.emit(new OffloadingDoneEvent(
          task.taskId));
      }
    });
  }

  @Override
  public void onData(Object event) {

    if (event instanceof OffloadingTask) {

      final OffloadingTask task = (OffloadingTask) event;

      final ReadyTask readyTask = findAndRemoveReadyTask(task.taskId);

      if (readyTask != null) {
        // just start
        startTask(readyTask, task);
      } else {
        LOG.info("Pending task {}", task.taskId);
        synchronized (pendingTask) {
          pendingTask.add(task);
        }
      }

    } else if (event instanceof ReadyTask) {

      final ReadyTask readyTask = (ReadyTask) event;
      final OffloadingTask task = findAndRemoveOffloadingTask(readyTask.taskId);

      if (task != null) {
        startTask(readyTask, task);
      } else {
        // the task is not submitted yet
        LOG.info("Task is not submitted yet {}", readyTask.taskId);
        synchronized (readyTasks) {
          readyTasks.add(readyTask);
        }
      }

    } else if (event instanceof TaskEndEvent) {
      // TODO
      final TaskEndEvent endEvent = (TaskEndEvent) event;
      final TaskExecutor deletedTask = findTask(endEvent.taskId);
      final ExecutorThread executorThread = taskAssignedMap.remove(deletedTask);
      taskExecutorStartTimeMap.remove(deletedTask);

      executorThread.deleteTask(deletedTask);

    } else {
      throw new RuntimeException("Unsupported event type: " + event);
    }
  }

  private TaskExecutor findTask(final String taskId) {
    while (true) {
      for (final TaskExecutor taskExecutor : taskAssignedMap.keySet()) {
        if (taskExecutor.getId().equals(taskId)) {
          return taskExecutor;
        }
      }

      LOG.info("Not finding {}... waiting for task start", taskId);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void close() {
     // TODO: 1 disconnect relay server channel
    prepareService.shutdown();

    executorThreads.forEach(executor -> {
      executor.close();
    });

    try {
      executorGlobalInstances.close();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    LOG.info("Shutting down prepare service");
    try {
      prepareService.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    LOG.info("End of Shutting down prepare service");

    scheduledExecutorService.shutdown();
    try {
      scheduledExecutorService.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    byteTransport.close();

    relayServerClient.close();
    rendevousServerClient.close();
    LOG.info("End of byte transport");
  }
}
