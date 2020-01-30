package org.apache.nemo.offloading.workers.vm;

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
import org.apache.nemo.offloading.common.LambdaRuntimeContext;
import org.apache.nemo.offloading.common.OffloadingOutputCollector;
import org.apache.nemo.offloading.common.OffloadingTransform;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingHeartbeatEvent;
import org.apache.nemo.runtime.lambdaexecutor.ReadyTask;
import org.apache.nemo.runtime.lambdaexecutor.ThrottlingEvent;
import org.apache.nemo.runtime.lambdaexecutor.datatransfer.*;
import org.apache.nemo.runtime.lambdaexecutor.downstream.TaskEndEvent;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingTask;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingTaskExecutor;
import org.apache.reef.io.network.naming.NameResolver;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerAddr;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerPort;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public final class VmOffloadingExecutor implements OffloadingTransform<Object, Object> {

  private static final Logger LOG = LoggerFactory.getLogger(VmOffloadingExecutor.class.getName());

  private final int executorThreadNum;
  private List<ExecutorThread> executorThreads;
  private ScheduledExecutorService scheduledExecutorService;

  private final AtomicInteger receivedTasks = new AtomicInteger(0);

  private final ConcurrentMap<SocketChannel, Boolean> channels;
  private String executorId;

  private final Map<String, InetSocketAddress> executorAddressMap;
  private final Map<String, Serializer> serializerMap;
  private final Map<String, String> taskExecutorIdMap;


  private transient ScalingByteTransport byteTransport;
  private transient IntermediateDataIOFactory intermediateDataIOFactory;
  private transient PipeManagerWorker pipeManagerWorker;
  private transient OffloadingOutputCollector oc;
  private transient VMScalingClientTransport clientTransport;
  private transient AckScheduledService ackScheduledService;

  private final ConcurrentMap<TaskExecutor, ExecutorThread> taskAssignedMap;
  private final ConcurrentMap<TaskExecutor, Long> taskExecutorStartTimeMap;
  private final Map<TransferKey, Integer> taskTransferIndexMap;

  private final int rendevousServerPort;

  private transient RendevousServerClient rendevousServerClient;
  private final TaskLocationMap taskLocationMap;
  private final Map<String, TaskLoc> taskLocMap;

  private transient ExecutorService prepareService;

  private transient ExecutorGlobalInstances executorGlobalInstances;

  private transient OutputWriterFlusher outputWriterFlusher;

  private final String rendevousServerAddress;

  private transient ExecutorService executorStartService;

  private final TaskLoc myLocation;

  private final String nameServerAddr;
  private final int nameServerPort;

  private final NameResolver nameResolver;

  public VmOffloadingExecutor(final int executorThreadNum,
                              final Map<String, InetSocketAddress> executorAddressMap,
                              final Map<String, Serializer> serializerMap,
                              final Map<String, String> taskExecutorIdMap,
                              final Map<TransferKey, Integer> taskTransferIndexMap,
                              final String rendevousServerAddress,
                              final int rendevousServerPort,
                              final String nameServerAddr,
                              final int nameServerPort,
                              final String executorId,
                              final TaskLoc myLocation) {
    this.executorThreadNum = executorThreadNum;
    this.channels = new ConcurrentHashMap<>();
    this.executorId = executorId;
    this.executorAddressMap = executorAddressMap;
    this.serializerMap = serializerMap;
    this.taskExecutorIdMap = taskExecutorIdMap;
    this.taskTransferIndexMap = taskTransferIndexMap;
    this.taskAssignedMap = new ConcurrentHashMap<>();
    this.taskExecutorStartTimeMap = new ConcurrentHashMap<>();
    this.rendevousServerAddress = rendevousServerAddress;
    this.rendevousServerPort = rendevousServerPort;
    this.nameServerAddr = nameServerAddr;
    this.nameServerPort = nameServerPort;
    this.taskLocationMap = new TaskLocationMap();
    this.taskLocMap = taskLocationMap.locationMap;
    this.myLocation = myLocation;

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(NameResolverNameServerAddr.class, nameServerAddr);
    jcb.bindNamedParameter(NameResolverNameServerPort.class, nameServerPort + "");
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    try {
      this.nameResolver = injector.getInstance(NameResolver.class);
    } catch (InjectionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
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

   LOG.info("ExecutorIdMap: {}", taskExecutorIdMap);

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
        new ExecutorThread(1, "lambda-" + i));
      executorThreads.get(i).start();
    }

    final NativeChannelImplementationSelector selector = new NativeChannelImplementationSelector();
    final ControlFrameEncoder controlFrameEncoder = new ControlFrameEncoder();
    final DataFrameEncoder dataFrameEncoder = new DataFrameEncoder();

    this.clientTransport = new VMScalingClientTransport();

    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.scheduleAtFixedRate(() -> {

      final List<Pair<String, TaskMetrics.RetrievedMetrics>> taskMetricsList = new ArrayList<>(taskAssignedMap.size());

      for (final TaskExecutor taskExecutor : taskAssignedMap.keySet()) {
        taskMetricsList.add(Pair.of(taskExecutor.getId(),
          taskExecutor.getTaskMetrics().retrieve(taskExecutor.getNumKeys())));
      }

      if (!taskMetricsList.isEmpty()) {
        final OperatingSystemMXBean bean =
          (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        final double cpuLoad = bean.getProcessCpuLoad();

        outputCollector.emit(new OffloadingHeartbeatEvent(executorId, taskMetricsList,
          cpuLoad / ScalingPolicyParameters.VM_LAMBDA_CPU_LOAD_RATIO));

        for (final SocketChannel channel : channels.keySet()) {
          LOG.info("Flush {} channels: {}", channels.size(), channels.keySet());
          if (channel.isOpen()) {
            channel.flush();
          }
        }
      }

    }, 1, 1, TimeUnit.SECONDS);

    final ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    final ConcurrentMap<Integer, ByteInputContext> inputContexts = new ConcurrentHashMap<>();
    final ConcurrentMap<Integer, ByteOutputContext> outputContexts = new ConcurrentHashMap<>();

    final LambdaByteTransportChannelInitializer initializer =
      new LambdaByteTransportChannelInitializer(channelGroup,
        controlFrameEncoder, dataFrameEncoder, channels, executorId, ackScheduledService,
        taskTransferIndexMap, inputContexts, outputContexts,
        outputWriterFlusher, myLocation, taskLocationMap);

    // this executor is running on vm scaling worker
    this.rendevousServerClient = new RendevousServerClient(rendevousServerAddress, rendevousServerPort);

    byteTransport = new VMScalingLambdaByteTransport(
      executorId, selector, initializer, executorAddressMap, channelGroup, "none",
      0, nameResolver, false);

    final ByteTransfer byteTransfer = new ByteTransfer(byteTransport, executorId);

    initializer.setByteTransfer(byteTransfer);

    pipeManagerWorker =
      new PipeManagerWorker(executorId, byteTransfer, taskExecutorIdMap, serializerMap, taskLocMap, null, false);

    intermediateDataIOFactory =
      new IntermediateDataIOFactory(pipeManagerWorker);
  }

  private TaskExecutor findTaskExecutor(final String taskId) {
    for (final TaskExecutor taskExecutor : taskAssignedMap.keySet()) {
      if (taskExecutor.getId().equals(taskId)) {
        return taskExecutor;
      }
    }

    throw new RuntimeException("Cannot find task executor " + taskId);
  }

  @Override
  public void onData(Object event) {

    if (event instanceof OffloadingTask) {
      final OffloadingTask task = (OffloadingTask) event;

      LOG.info("Start task {}", task.taskId);

      final int executorIndex = receivedTasks.getAndIncrement() % executorThreadNum;
      final ExecutorThread executorThread = executorThreads.get(executorIndex);

      LOG.info("Receive task {}, assign it to executor-{}", task.taskId, executorIndex);

      final OffloadingTaskExecutor taskExecutor = new OffloadingTaskExecutor(
        task,
        serializerMap,
        intermediateDataIOFactory,
        oc,
        prepareService,
        executorGlobalInstances,
        rendevousServerClient,
        executorThread);

      taskAssignedMap.put(taskExecutor, executorThread);

      LOG.info("Pending task {}", task.taskId);

      // Emit offloading done
      synchronized (oc) {
        oc.emit(new OffloadingDoneEvent(
          task.taskId));
      }

    } else if (event instanceof ReadyTask) {
      LOG.info("Receive ready task {}", ((ReadyTask) event).taskId);
      final ReadyTask readyTask = (ReadyTask) event;


      for (final Map.Entry<String, TaskLoc> entry : readyTask.taskLocationMap.entrySet()) {
        taskLocMap.put(entry.getKey(), entry.getValue());
      }

      LOG.info("TaskLocMap: {}", taskLocMap);


      final OffloadingTaskExecutor taskExecutor = (OffloadingTaskExecutor) findTaskExecutor(readyTask.taskId);
      taskExecutor.start(readyTask);

      final ExecutorThread executorThread = taskAssignedMap.get(taskExecutor);
      executorThread.addNewTask(taskExecutor);

    } else if (event instanceof TaskEndEvent) {
      // TODO
      final TaskEndEvent endEvent = (TaskEndEvent) event;
      final TaskExecutor deletedTask = findTask(endEvent.taskId);
      final ExecutorThread executorThread = taskAssignedMap.remove(deletedTask);
      taskExecutorStartTimeMap.remove(deletedTask);

      executorThread.deleteTask(deletedTask);

    } else if (event instanceof ThrottlingEvent) {

      LOG.info("Get throttling");

      scheduledExecutorService.schedule(() -> {
        executorThreads.forEach(thread -> {
          thread.getThrottle().set(true);
        });
      }, 10, TimeUnit.MILLISECONDS);

      scheduledExecutorService.schedule(() -> {
        executorThreads.forEach(thread -> {
          thread.getThrottle().set(false);
        });
      }, 500, TimeUnit.MILLISECONDS);

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

    rendevousServerClient.close();
    LOG.info("End of byte transport");
  }
}
