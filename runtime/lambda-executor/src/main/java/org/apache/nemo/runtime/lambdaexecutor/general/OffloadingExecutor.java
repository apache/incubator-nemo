package org.apache.nemo.runtime.lambdaexecutor.general;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.log4j.Level;
import org.apache.nemo.common.*;
import org.apache.nemo.common.Pair;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.Executor;
import org.apache.nemo.runtime.executor.common.ExecutorMetrics;
import org.apache.nemo.runtime.executor.common.controlmessages.offloading.SendToOffloadingWorker;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.nemo.runtime.executor.common.tasks.TaskExecutor;
import org.apache.nemo.runtime.lambdaexecutor.*;
import org.apache.nemo.runtime.lambdaexecutor.datatransfer.*;
import org.apache.nemo.runtime.message.MessageEnvironment;
import org.apache.nemo.runtime.message.MessageParameters;
import org.apache.nemo.runtime.message.netty.NettyWorkerEnvironment;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;



public final class OffloadingExecutor implements OffloadingTransform<Object, Object> {

  private static final Logger LOG = LoggerFactory.getLogger(OffloadingExecutor.class.getName());

  private final int executorThreadNum;
  private final ConcurrentMap<SocketChannel, Boolean> channels;
  private final AtomicInteger numReceivedTasks = new AtomicInteger(0);
  private final Map<String, Double> samplingMap;
  private final boolean isLocalSource;
  private final int stateStorePort;

  private List<ExecutorThread> executorThreads;
  private StateStore stateStore;
  private VMScalingClientTransport clientTransport;
  private Channel parentExecutorChannel;
  private IntermediateDataIOFactory intermediateDataIOFactory;
  private ExecutorService prepareService;
  private PipeManagerWorker pipeManagerWorker;
  private OutputCollectorGenerator outputCollectorGenerator;


  // updated whenever task is submitted
  private final SerializerManager serializerManager;
  private final Map<Triple<String, String, String>, Integer> indexMap;
  private final Map<Integer, String> indexTaskMap;
  private final Map<String, ExecutorThread> taskExecutorThreadMap;
  private final Map<String, TaskExecutor> taskExecutorMap;

  private ScheduledExecutorService scheduledService;

  private long throttleRate;
  private ExecutorMetrics executorMetrics;
  private MonitoringThread monitoringThread;

  private final String nameServerAddr;
  private final int nameServerPort;
  private final String executorId;
  private final int flushPeriod;

  private Executor executor;
  private final boolean controlLogging;
  private final long latencyLimit;
  private final String optimizationPolicy;
  private final boolean ec2;


  public OffloadingExecutor(final int executorThreadNum,
                            final Map<String, Double> samplingMap,
                            final boolean isLocalSource,
                            final int stateStorePort,
                            final String nameServerAddr,
                            final int nameServerPort,
                            final String executorId,
                            final int flushPeriod,
                            final boolean controlLogging,
                            final long latencyLimit,
                            final boolean ec2,
                            final String optimizationPolicy) {
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
    LOG.info("Offloading executor started {}/{}/{}/{}/{}/{}",
      executorThreadNum, samplingMap, isLocalSource);
    this.ec2 = ec2;
    this.stateStorePort = stateStorePort;
    this.executorThreadNum = executorThreadNum;
    this.channels = new ConcurrentHashMap<>();
    this.samplingMap = samplingMap;
    this.isLocalSource = isLocalSource;
    this.serializerManager = new DefaultSerializerManagerImpl();
    this.indexMap = new ConcurrentHashMap<>();
    this.indexTaskMap = new ConcurrentHashMap<>();
    this.taskExecutorThreadMap = new ConcurrentHashMap<>();
    this.taskExecutorMap = new ConcurrentHashMap<>();
    this.controlLogging = controlLogging;
    this.latencyLimit = latencyLimit;
    this.optimizationPolicy = optimizationPolicy;

    this.nameServerAddr = nameServerAddr;
    this.nameServerPort = nameServerPort;
    this.executorId = executorId;
    this.flushPeriod = flushPeriod;
  }

  public void encode(final DataOutputStream dos) {
    try {
      dos.writeInt(executorThreadNum);
      dos.writeInt(samplingMap.size());
      for (String key : samplingMap.keySet()) {
        dos.writeUTF(key);
        dos.writeDouble(samplingMap.get(key));
      }
      dos.writeBoolean(isLocalSource);
      dos.writeInt(stateStorePort);

      dos.writeUTF(nameServerAddr);
      dos.writeInt(nameServerPort);
      dos.writeUTF(executorId);
      dos.writeInt(flushPeriod);

      dos.writeBoolean(controlLogging);
      dos.writeLong(latencyLimit);
      dos.writeBoolean(ec2);

      dos.writeUTF(optimizationPolicy);

    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static OffloadingExecutor decode(final DataInputStream dis) {
    try {
      final int executorThreadNum = dis.readInt();
      final int size = dis.readInt();
      final Map<String, Double> samplingMap = new HashMap<>();
      for (int i = 0; i < size; i++) {
        final String key = dis.readUTF();
        final Double val = dis.readDouble();
        samplingMap.put(key, val);
      }
      final boolean isLocalSource = dis.readBoolean();
      final int stateStorePort = dis.readInt();

      final String nameServerAddr = dis.readUTF();
      final int nameServerPort = dis.readInt();
      final String executorId = dis.readUTF();
      final int flushPeriod = dis.readInt();
      final boolean controlLogging = dis.readBoolean();
      final long latencyLimit = dis.readLong();
      final boolean ec2 = dis.readBoolean();
      final String optimizationPolicy = dis.readUTF();


      return new OffloadingExecutor(executorThreadNum, samplingMap, isLocalSource,
        stateStorePort,
        nameServerAddr, nameServerPort, executorId, flushPeriod, controlLogging,
        latencyLimit,
        ec2,
        optimizationPolicy);
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private final AtomicLong prevProcessingSum = new AtomicLong(0);
  LambdaRuntimeContext context;

  @Override
  public boolean hasRemainingEvent() {
    for (ExecutorThread e : executorThreads) {
      if (!e.isEmpty()) {
        return true;
      }
    }

    return false;
  }

  @Override
  public void shutdownSchedule() {
    scheduledService.shutdown();
    executor.deactivate();
  }

  @Override
  public void schedule() {
    this.scheduledService = Executors.newSingleThreadScheduledExecutor();
    executor.activate();

    /*
    this.scheduledService.scheduleAtFixedRate(() -> {
      if (parentExecutorChannel != null && parentExecutorChannel.isOpen()) {
        parentExecutorChannel.flush();
      }
    }, 50, 50, TimeUnit.MILLISECONDS);

    this.scheduledService.scheduleAtFixedRate(() -> {
      LOG.info("CPU Load {}", monitoringThread.getTotalUsage());
    }, 10, 1000, TimeUnit.MILLISECONDS);

    final Channel controlChannel = context.getControlChannel();

    final OperatingSystemMXBean operatingSystemMXBean =
      (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();


    scheduledService.scheduleAtFixedRate(() -> {
      long inputSum = 0L;
      long processSum = 0L;

      for (final String key : executorMetrics.taskInputReceiveRateMap.keySet()) {
        inputSum += executorMetrics.taskInputReceiveRateMap.get(key).left().get();
        processSum += executorMetrics.taskInputReceiveRateMap.get(key).right().get();
      }


      if (context.testing) {
        if (processSum == 0) {
          executorMetrics.load = 0;
        } else {
          executorMetrics.load = inputSum / processSum;
        }
      } else {
        executorMetrics.load = operatingSystemMXBean.getProcessCpuLoad();
      }

      executorMetrics.processingRate = processSum - prevProcessingSum.get();
      prevProcessingSum.set(processSum);

      final ByteBuf byteBuf = controlChannel.alloc().ioBuffer();
      final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);
      final DataOutputStream dos = new DataOutputStream(bos);
      executorMetrics.encode(dos);

      try {
        dos.close();
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      controlChannel.writeAndFlush(new OffloadingExecutorControlEvent(
        OffloadingExecutorControlEvent.Type.EXECUTOR_METRICS, byteBuf));

      LOG.info("worker {} processed offloaded event {}, received byte {}",
        ((LambdaRuntimeContext) context).requestId, processSum, ((OffloadingPipeManagerWorkerImpl) pipeManagerWorker).byteReceived);

    }, 10, 1000, TimeUnit.MILLISECONDS);
     */
  }

  @Override
  public void prepare(OffloadingContext c, OffloadingOutputCollector outputCollector) {
    context = (LambdaRuntimeContext)c;

    if (ec2) {
      this.monitoringThread = new MonitoringThread(1000);
    } else {
      this.monitoringThread = new MonitoringThread(1000, 1.0);
    }

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(EvalConf.ExecutorOnLambda.class, Boolean.toString(true));
    jcb.bindNamedParameter(MessageParameters.SenderId.class, executorId);

    final ObjectMapper objectMapper = new ObjectMapper();
    try {
      jcb.bindNamedParameter(EvalConf.SamplingJsonString.class, objectMapper.writeValueAsString(samplingMap));
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    jcb.bindNamedParameter(EvalConf.Ec2.class, Boolean.toString(ec2));

    jcb.bindNamedParameter(MessageParameters.NameServerAddr.class, nameServerAddr);
    jcb.bindNamedParameter(MessageParameters.NameServerPort.class, Integer.toString(nameServerPort));

    jcb.bindImplementation(MessageEnvironment.class, NettyWorkerEnvironment.class);
    jcb.bindImplementation(ByteTransport.class, LambdaByteTransport.class);

    jcb.bindNamedParameter(JobConf.ExecutorId.class, executorId);
    jcb.bindImplementation(SerializerManager.class, DefaultSerializerManagerImpl.class);
    jcb.bindNamedParameter(EvalConf.FlushPeriod.class, Integer.toString(flushPeriod));
    jcb.bindNamedParameter(EvalConf.ExecutorThreadNum.class, Integer.toString(executorThreadNum));

    jcb.bindImplementation(StateStore.class, NettyVMStateStoreClient.class);
    jcb.bindNamedParameter(NettyVMStateStoreClient.NettyVMStoreAddr.class, nameServerAddr);
    jcb.bindNamedParameter(NettyVMStateStoreClient.NettyVMStorePort.class, Integer.toString(stateStorePort));

    jcb.bindNamedParameter(EvalConf.ControlLogging.class, Boolean.toString(controlLogging));
    jcb.bindNamedParameter(JobConf.OptimizationPolicy.class, optimizationPolicy);

    jcb.bindNamedParameter(EvalConf.LatencyLimit.class, Long.toString(latencyLimit));


    final Configuration conf = jcb.build();

    final Injector injector = Tang.Factory.getTang().newInjector(conf);

    try {
      executor = injector.getInstance(Executor.class);
      executor.start();
    } catch (InjectionException e) {
      e.printStackTrace();
    }

    /*
    this.executorMetrics = new ExecutorMetrics();
    this.throttleRate = context.throttleRate;
    this.prepareService = Executors.newCachedThreadPool();

    LOG.info("Netty state store client before created for connectiong {} / {} ...",
      parentExecutorAddress, stateStorePort);

    // final LambdaRuntimeContext runtimeContext = (LambdaRuntimeContext) context;
    this.stateStore = new NettyVMStateStoreClient(parentExecutorAddress, stateStorePort);

    LOG.info("Netty state store client created...");

    pipeManagerWorker =
      new OffloadingPipeManagerWorkerImpl(executorId, indexMap, indexTaskMap);

    LOG.info("Pipe manager worker created...");

    this.intermediateDataIOFactory = new OffloadingIntermediateDataIOFactory(
      pipeManagerWorker, serializerManager);

    LOG.info("Intermediate data Io created...");

    this.outputCollectorGenerator =
      new OffloadingOutputCollectorGeneratorImpl(intermediateDataIOFactory, executorId + "-offloading");


    final OffloadingTransportChannelInitializer initializer =
      new OffloadingTransportChannelInitializer(pipeManagerWorker,
        ((LambdaRuntimeContext) c).handler);

    LOG.info("OffloadingTransportChannelInitializer...");

    this.clientTransport = new VMScalingClientTransport(initializer);

    this.parentExecutorChannel = clientTransport
      .connectTo(parentExecutorAddress, parentExecutorDataPort).channel();

    LOG.info("Data channel: {}", parentExecutorAddress);

    final OffloadingTaskControlEventHandlerImpl taskControlEventHandler =
      new OffloadingTaskControlEventHandlerImpl(executorId, pipeManagerWorker, taskExecutorThreadMap,
        taskExecutorMap, parentExecutorChannel, context.getControlChannel());

    executorThreads = new ArrayList<>();
    for (int i = 0; i < executorThreadNum; i++) {
      executorThreads.add(
        new ExecutorThread(1,
          "lambda-" + i,
          taskControlEventHandler,
          throttleRate,
          executorMetrics,
          context.testing));
      executorThreads.get(i).start();
    }
    */

    LOG.info("Executor thread created: {}", executorId);
  }

  /*
  @Override
  public Channel getDataChannel() {
    return parentExecutorChannel;
  }

  public String getDataChannelAddr() {
    return parentExecutorChannel.localAddress().toString();
  }

  @Override
  public int getDataChannelPort() {
    return Integer.valueOf(parentExecutorChannel.localAddress().toString().split(":")[1]);
  }
  */

  @Override
  public void onData(Object event, OffloadingOutputCollector a) {
    if (event instanceof SendToOffloadingWorker) {
      // prepareOffloading task
      final SendToOffloadingWorker e = (SendToOffloadingWorker) event;
      LOG.info("IndexMap: {}", e.indexMap);
      final ByteArrayInputStream bis = new ByteArrayInputStream(e.taskByte);
      final DataInputStream dis = new DataInputStream(bis);
      final long st = System.currentTimeMillis();
      try {
        final Task task;
        final String stageId = RuntimeIdManager.getStageIdFromTaskId(e.taskId);

        if (context.stageTaskMap.containsKey(stageId)) {
          task = Task.decode(dis, context.stageTaskMap.get(stageId));
          LOG.info("Decode task from task caching");
        } else {
          task = Task.decode(dis);
          context.stageTaskMap.put(stageId,
            new TaskCaching(task.getTaskIncomingEdges(),
              task.getTaskOutgoingEdges(),
              task.getExecutionProperties()));
        }

        indexMap.putAll(e.indexMap);
        e.indexMap.forEach((key, index) -> {
          indexTaskMap.put(index, key.getRight());
        });

        final long et = System.currentTimeMillis();

        LOG.info("Offload Executor [{}] received Task [{}] to execute. time {}",
          new Object[]{executorId, task.getTaskId(), et - st});

        // final DataInputStream diss = new DataInputStream(new ByteArrayInputStream(task.getSerializedIRDag()));
        // final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag =
        //  DAG.decode(diss);

        throw new RuntimeException("TODO: launchTask");
        // launchTask(task, task.getIrDag(), e.offloaded);
      } catch (Exception e1) {
        e1.printStackTrace();
        throw new RuntimeException(e1);
      }

    } else {
      throw new RuntimeException("invalid event " + event);
    }
  }

  @Override
  public void close() {
    scheduledService.shutdown();
    stateStore.close();
    clientTransport.close();
  }


  /*
  private void launchTask(final Task task,
                          final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag,
                          final boolean offloaded) {

    task.getTaskIncomingEdges().forEach(e -> serializerManager.register(e.getId(),
      getEncoderFactory(e.getPropertyValue(EncoderProperty.class).get()),
      getDecoderFactory(e.getPropertyValue(DecoderProperty.class).get()),
      e.getPropertyValue(CompressionProperty.class).orElse(null),
      e.getPropertyValue(DecompressionProperty.class).orElse(null)));
    task.getTaskOutgoingEdges().forEach(e -> serializerManager.register(e.getId(),
      getEncoderFactory(e.getPropertyValue(EncoderProperty.class).get()),
      getDecoderFactory(e.getPropertyValue(DecoderProperty.class).get()),
      e.getPropertyValue(CompressionProperty.class).orElse(null),
      e.getPropertyValue(DecompressionProperty.class).orElse(null)));

    LOG.info("{} Launch task: {}", executorId, task.getTaskId());

    try {

      final int numTask = numReceivedTasks.getAndIncrement();
      final int index = numTask % executorThreadNum;
      final ExecutorThread executorThread = executorThreads.get(index);

      final TaskExecutor taskExecutor =
      new DefaultTaskExecutorImpl(
        Thread.currentThread().getId(),
        executorId,
        task,
        irDag,
        intermediateDataIOFactory,
        serializerManager,
        null,
        samplingMap,
        isLocalSource,
        prepareService,
        executorThread,
        pipeManagerWorker,
        stateStore,
        new SimpleOffloadingManager(),
        pipeManagerWorker,
        outputCollectorGenerator,
        new byte[0],
        new NoOffloadingPreparer(),
        offloaded);

      LOG.info("Add Task {} to {} thread of {}", taskExecutor.getId(), index, executorId);
      executorThreads.get(index).addNewTask(taskExecutor);

      taskExecutorThreadMap.put(taskExecutor.getId(), executorThread);
      taskExecutorMap.put(taskExecutor.getId(), taskExecutor);

      LOG.info("Task Executor Map : {}", taskExecutorMap.keySet());

      //taskExecutor.execute();
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
  */
}
