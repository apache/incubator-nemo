package org.apache.nemo.runtime.lambdaexecutor.general;

import com.sun.management.OperatingSystemMXBean;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.nemo.common.*;
import org.apache.nemo.common.coder.FSTSingleton;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.executionproperty.CompressionProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DecoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DecompressionProperty;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.offloading.common.*;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.ExecutorMetrics;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.runtime.executor.common.controlmessages.offloading.SendToOffloadingWorker;
import org.apache.nemo.runtime.executor.common.controlmessages.offloading.TaskFinish;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.nemo.runtime.lambdaexecutor.*;
import org.apache.nemo.runtime.lambdaexecutor.datatransfer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.nemo.runtime.executor.common.TaskExecutorUtil.getDecoderFactory;
import static org.apache.nemo.runtime.executor.common.TaskExecutorUtil.getEncoderFactory;


public final class OffloadingExecutor implements OffloadingTransform<Object, Object> {

  private static final Logger LOG = LoggerFactory.getLogger(OffloadingExecutor.class.getName());

  private final int executorThreadNum;
  private final ConcurrentMap<SocketChannel, Boolean> channels;
  private final String executorId;
  private final String parentExecutorAddress;
  private final int parentExecutorDataPort;
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
  private final Map<String, ExecutorThread> taskExecutorThreadMap;
  private final Map<String, TaskExecutor> taskExecutorMap;

  private ScheduledExecutorService scheduledService;

  private long throttleRate;
  private ExecutorMetrics executorMetrics;


  public OffloadingExecutor(final int executorThreadNum,
                            final Map<String, Double> samplingMap,
                            final boolean isLocalSource,
                            final String parentExecutorId,
                            final String parentExecutorAddress,
                            final int parentExecutorDataPort,
                            final int stateStorePort) {
    LOG.info("Offloading executor started {}/{}/{}/{}/{}/{}",
      executorThreadNum, samplingMap, isLocalSource, parentExecutorId, parentExecutorAddress, parentExecutorDataPort);
    this.stateStorePort = stateStorePort;
    this.executorThreadNum = executorThreadNum;
    this.channels = new ConcurrentHashMap<>();
    this.executorId = parentExecutorId;
    this.samplingMap = samplingMap;
    this.isLocalSource = isLocalSource;
    this.serializerManager = new DefaultSerializerManagerImpl();
    this.indexMap = new ConcurrentHashMap<>();
    this.parentExecutorAddress = parentExecutorAddress;
    this.parentExecutorDataPort = parentExecutorDataPort;
    this.taskExecutorThreadMap = new ConcurrentHashMap<>();
    this.taskExecutorMap = new ConcurrentHashMap<>();
  }

  private final AtomicLong prevProcessingSum = new AtomicLong(0);

  @Override
  public void prepare(OffloadingContext c, OffloadingOutputCollector outputCollector) {
    final LambdaRuntimeContext context = (LambdaRuntimeContext)c;

    this.executorMetrics = new ExecutorMetrics();
    this.throttleRate = context.throttleRate;
    this.prepareService = Executors.newCachedThreadPool();
    this.scheduledService = Executors.newSingleThreadScheduledExecutor();
    this.scheduledService.scheduleAtFixedRate(() -> {
      if (parentExecutorChannel != null && parentExecutorChannel.isOpen()) {
        parentExecutorChannel.flush();
      }
    }, 10, 10, TimeUnit.MILLISECONDS);

    // final LambdaRuntimeContext runtimeContext = (LambdaRuntimeContext) context;
    this.stateStore = new NettyVMStateStoreClient(parentExecutorAddress, stateStorePort);


    final NativeChannelImplementationSelector selector = new NativeChannelImplementationSelector();
    final ControlFrameEncoder controlFrameEncoder = new ControlFrameEncoder();
    final DataFrameEncoder dataFrameEncoder = new DataFrameEncoder();

    pipeManagerWorker =
      new OffloadingPipeManagerWorkerImpl(executorId, indexMap);

    this.intermediateDataIOFactory = new OffloadingIntermediateDataIOFactory(
      pipeManagerWorker, serializerManager);

    this.outputCollectorGenerator =
      new OffloadingOutputCollectorGeneratorImpl(intermediateDataIOFactory);

    final OffloadingTransportChannelInitializer initializer =
      new OffloadingTransportChannelInitializer(pipeManagerWorker,
        new ControlMessageHandler());

    this.clientTransport = new VMScalingClientTransport(initializer);

    this.parentExecutorChannel = clientTransport
      .connectTo(parentExecutorAddress, parentExecutorDataPort).channel();


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

    final Channel controlChannel = context.getControlChannel();


    final OperatingSystemMXBean operatingSystemMXBean =
      (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    scheduledService.scheduleAtFixedRate(() -> {
      long inputSum = 0L;
      long processSum = 0L;

      for (final String key : executorMetrics.taskInputProcessRateMap.keySet()) {
        inputSum += executorMetrics.taskInputProcessRateMap.get(key).left().get();
        processSum += executorMetrics.taskInputProcessRateMap.get(key).right().get();
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
      try {
        FSTSingleton.getInstance().encodeToStream(bos, executorMetrics);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }


      controlChannel.writeAndFlush(new OffloadingEvent(
        OffloadingEvent.Type.EXECUTOR_METRICS, byteBuf));
    }, 1, 1, TimeUnit.SECONDS);

  }

  public String getDataChannelAddr() {
    return parentExecutorChannel.localAddress().toString();
  }

  @Override
  public void onData(Object event, OffloadingOutputCollector a) {
    if (event instanceof SendToOffloadingWorker) {
      // prepareOffloading task
      final SendToOffloadingWorker e = (SendToOffloadingWorker) event;
      LOG.info("IndexMap: {}", e.indexMap);
      final ByteArrayInputStream bis = new ByteArrayInputStream(e.taskByte);
      try {
        final Task task = (Task) FSTSingleton.getInstance().decodeFromStream(bis);
        indexMap.putAll(e.indexMap);

        LOG.info("Offload Executor [{}] received Task [{}] to execute.",
          new Object[]{executorId, task.getTaskId()});

        final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag =
          (DAG) FSTSingleton.getInstance().asObject(task.getSerializedIRDag());

        launchTask(task, irDag, e.offloaded);
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
    irDag.getVertices().forEach(v -> {
      irDag.getOutgoingEdgesOf(v).forEach(e -> serializerManager.register(e.getId(),
        getEncoderFactory(e.getPropertyValue(EncoderProperty.class).get()),
        getDecoderFactory(e.getPropertyValue(DecoderProperty.class).get()),
        e.getPropertyValue(CompressionProperty.class).orElse(null),
        e.getPropertyValue(DecompressionProperty.class).orElse(null)));
    });

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
        offloaded);

      LOG.info("Add Task {} to {} thread of {}", taskExecutor.getId(), index, executorId);
      executorThreads.get(index).addNewTask(taskExecutor);

      taskExecutorThreadMap.put(taskExecutor.getId(), executorThread);
      taskExecutorMap.put(taskExecutor.getId(), taskExecutor);


      //taskExecutor.execute();
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public final class ControlMessageHandler extends SimpleChannelInboundHandler<TaskControlMessage> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TaskControlMessage msg) throws Exception {
      throw new RuntimeException();
    }

      @Override
      public void channelActive(final ChannelHandlerContext ctx) {
        // channelGroup.add(ctx.channel());
        // outputWriterFlusher.registerChannel(ctx.channel());
      }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) {
      // channelGroup.remove(ctx.channel());
      // outputWriterFlusher.removeChannel(ctx.channel());
      LOG.info("Channel closed !! {}", ctx.channel());
    }
  }
}
