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
import org.apache.nemo.runtime.executor.common.ExecutorThread;
import org.apache.nemo.runtime.executor.common.Serializer;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.executor.common.TaskLocationMap;
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

  private final int executorThreadNum = 1;
  private List<ExecutorThread> executorThreads;
  private ScheduledExecutorService scheduledExecutorService;

  private final AtomicInteger receivedTasks = new AtomicInteger(0);

  private final ConcurrentMap<SocketChannel, Boolean> channels;
  private final String executorId;

  private final Map<String, InetSocketAddress> executorAddressMap;
  private final Map<String, Serializer> serializerMap;
  private final Map<Pair<String, Integer>, String> taskExecutorIdMap;


  private transient LambdaByteTransport byteTransport;
  private transient IntermediateDataIOFactory intermediateDataIOFactory;
  private transient PipeManagerWorker pipeManagerWorker;
  private transient OffloadingOutputCollector oc;
  private transient VMScalingClientTransport clientTransport;
  private transient AckScheduledService ackScheduledService;

  private final ConcurrentMap<TaskExecutor, ExecutorThread> taskAssignedMap;
  private final Map<TransferKey, Integer> taskTransferIndexMap;

  private final String relayServerAddress;
  private final int relayServerPort;

  private transient RelayServerClient relayServerClient;
  private final ConcurrentMap<NemoTriple<String, Integer, Boolean>, TaskLocationMap.LOC> taskLocMap;

  public OffloadingExecutor(final Map<String, InetSocketAddress> executorAddressMap,
                            final Map<String, Serializer> serializerMap,
                            final Map<Pair<String, Integer>, String> taskExecutorIdMap,
                            final Map<TransferKey, Integer> taskTransferIndexMap,
                            final String relayServerAddress,
                            final int relayServerPort,
                            final String executorId) {
    this.channels = new ConcurrentHashMap<>();
    this.executorId = executorId;
    this.executorAddressMap = executorAddressMap;
    this.serializerMap = serializerMap;
    this.taskExecutorIdMap = taskExecutorIdMap;
    this.taskTransferIndexMap = taskTransferIndexMap;
    this.taskAssignedMap = new ConcurrentHashMap<>();
    this.relayServerAddress = relayServerAddress;
    this.relayServerPort = relayServerPort;
    this.taskLocMap = new ConcurrentHashMap<>();

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

    final ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    final ConcurrentMap<Integer, ByteInputContext> inputContexts = new ConcurrentHashMap<>();
    final ConcurrentMap<Integer, ByteOutputContext> outputContexts = new ConcurrentHashMap<>();

    final LambdaByteTransportChannelInitializer initializer =
      new LambdaByteTransportChannelInitializer(channelGroup,
        controlFrameEncoder, dataFrameEncoder, channels, executorId, ackScheduledService,
        taskTransferIndexMap, inputContexts, outputContexts);

    // Relay server channel initializer
    final RelayServerClientChannelInitializer relayServerClientChannelInitializer =
      new RelayServerClientChannelInitializer(channelGroup,
        controlFrameEncoder, dataFrameEncoder, channels, executorId, ackScheduledService,
        taskTransferIndexMap, inputContexts, outputContexts);

    final EventLoopGroup clientGroup = new NioEventLoopGroup(2, new DefaultThreadFactory("relayClient"));
    final Bootstrap clientBootstrap = new Bootstrap()
      .group(clientGroup)
      .channel(NioSocketChannel.class)
      .handler(relayServerClientChannelInitializer)
      .option(ChannelOption.SO_REUSEADDR, true);

    this.relayServerClient = new RelayServerClient(clientGroup, clientBootstrap, relayServerAddress, relayServerPort);

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
     // TODO: 1 disconnect relay server channel
  }
}
