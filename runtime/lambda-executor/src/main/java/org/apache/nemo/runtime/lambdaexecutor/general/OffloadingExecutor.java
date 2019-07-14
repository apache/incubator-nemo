package org.apache.nemo.runtime.lambdaexecutor.general;

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
import org.apache.nemo.common.NemoTriple;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.offloading.common.OffloadingOutputCollector;
import org.apache.nemo.offloading.common.OffloadingTransform;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.nemo.runtime.lambdaexecutor.datatransfer.RelayServerClient;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingHeartbeatEvent;
import org.apache.nemo.runtime.lambdaexecutor.datatransfer.*;
import org.apache.nemo.runtime.lambdaexecutor.downstream.TaskEndEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
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

  private transient RelayServerClient relayServerClient;
  private final ConcurrentMap<NemoTriple<String, Integer, Boolean>, TaskLocationMap.LOC> taskLocMap;

  private transient ExecutorService prepareService;

  private transient ExecutorGlobalInstances executorGlobalInstances;

  private transient OutputWriterFlusher outputWriterFlusher;

  private final Map<String, Pair<String, Integer>> relayServerInfo;

  public OffloadingExecutor(final int executorThreadNum,
                            final Map<String, InetSocketAddress> executorAddressMap,
                            final Map<String, Serializer> serializerMap,
                            final Map<NemoTriple<String, Integer, Boolean>, String> taskExecutorIdMap,
                            final Map<TransferKey, Integer> taskTransferIndexMap,
                            final String relayServerAddress,
                            final int relayServerPort,
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
    this.taskLocMap = new ConcurrentHashMap<>();
    this.relayServerInfo = relayServerInfo;


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
    LOG.info("ExecutorAddressMap: {}", executorAddressMap);

    this.oc = outputCollector;
    this.ackScheduledService = new AckScheduledService();
    this.prepareService = Executors.newCachedThreadPool();
    this.executorGlobalInstances = new ExecutorGlobalInstances();
    this.outputWriterFlusher = new OutputWriterFlusher(200);

    executorThreads = new ArrayList<>();
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
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

    this.relayServerClient = new RelayServerClient(
      clientGroup, clientBootstrap, relayServerAddress, relayServerPort, relayServerInfo);

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

  @Override
  public void onData(Object event) {

    if (event instanceof OffloadingTask) {

      final int executorIndex = receivedTasks.getAndIncrement() % executorThreadNum;
      final ExecutorThread executorThread = executorThreads.get(executorIndex);

      final OffloadingTask task = (OffloadingTask) event;

      LOG.info("Receive task {}, assign it to executor-{}", task.taskId, executorIndex);

      for (final Map.Entry<NemoTriple<String, Integer, Boolean>, TaskLocationMap.LOC> entry
        : task.taskLocationMap.entrySet()) {
        taskLocMap.put(entry.getKey(), entry.getValue());
      }

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

      // Emit offloading done
      oc.emit(new OffloadingDoneEvent(
        taskExecutor.getId()));

      executorThread.addNewTask(taskExecutor);

      taskExecutorStartTimeMap.put(taskExecutor, System.currentTimeMillis());
      taskAssignedMap.put(taskExecutor, executorThread);

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
    LOG.info("End of byte transport");
  }
}
