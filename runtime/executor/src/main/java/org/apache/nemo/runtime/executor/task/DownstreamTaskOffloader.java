package org.apache.nemo.runtime.executor.task;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.compiler.frontend.beam.source.BeamUnboundedSourceVertex;
import org.apache.nemo.compiler.frontend.beam.transform.GBKPartialTransform;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.client.StreamingWorkerService;
import org.apache.nemo.offloading.common.OffloadingWorker;
import org.apache.nemo.offloading.common.OffloadingWorkerFactory;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.executor.TransformContextImpl;
import org.apache.nemo.runtime.executor.bytetransfer.ByteInputContext;
import org.apache.nemo.runtime.executor.common.DataFetcher;
import org.apache.nemo.runtime.executor.common.SourceVertexDataFetcher;
import org.apache.nemo.runtime.executor.common.datatransfer.ByteTransferContextSetupMessage;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.datatransfer.OutputWriter;
import org.apache.nemo.runtime.executor.datatransfer.TaskInputContextMap;
import org.apache.nemo.runtime.lambdaexecutor.downstream.DownstreamOffloadingSerializer;
import org.apache.nemo.runtime.lambdaexecutor.downstream.DownstreamOffloadingTransform;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutput;
import org.apache.nemo.runtime.lambdaexecutor.middle.MiddleOffloadingSerializer;
import org.apache.nemo.runtime.lambdaexecutor.middle.MiddleOffloadingTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

// It receives data from upstream parents
// and emits results to downstream tasks
public final class DownstreamTaskOffloader implements Offloader {
  private static final Logger LOG = LoggerFactory.getLogger(DownstreamTaskOffloader.class.getName());

  private final StreamingWorkerService streamingWorkerService;

  private final byte[] serializedDag;
  private final OffloadingWorkerFactory offloadingWorkerFactory;
  private final Map<String, List<String>> taskOutgoingEdges;
  private final SerializerManager serializerManager;
  private final ConcurrentLinkedQueue<Object> offloadingEventQueue;
  private final List<SourceVertexDataFetcher> sourceVertexDataFetchers;
  private final String taskId;
  final ExecutorService closeService = Executors.newSingleThreadExecutor();


  private static final AtomicInteger sourceId = new AtomicInteger(0);

  private final List<OffloadingWorker> runningWorkers = new ArrayList<>();
  final ConcurrentMap<Integer, KafkaOffloadingDataEvent> offloadedDataFetcherMap = new ConcurrentHashMap<>();
  final Queue<KafkaOffloadingRequestEvent> kafkaOffloadPendingEvents = new LinkedBlockingQueue<>();

  private final AtomicReference<TaskExecutor.Status> taskStatus;

  private final ScheduledExecutorService logger = Executors.newSingleThreadScheduledExecutor();

  private final AtomicLong prevOffloadStartTime;
  private final AtomicLong prevOffloadEndTime;

  private final Map<String, InetSocketAddress> executorAddressMap;
  private final Map<Integer, String> dstTaskIndexTargetExecutorMap;
  private final String executorId;
  private final Task task;

  private final EvalConf evalConf;
  private final PersistentConnectionToMasterMap toMaster;
  private final Collection<OutputWriter> outputWriters;
  final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag;


  private final List<KafkaOffloadingOutput> kafkaOffloadingOutputs = new ArrayList<>();
  private final TaskExecutor taskExecutor;

  private int offloadingDataCnt = 0;

  private final ConcurrentMap<Integer, Long> taskTimeMap;
  private final TaskInputContextMap taskInputContextMap;

  public DownstreamTaskOffloader(final String executorId,
                                 final Task task,
                                 final TaskExecutor taskExecutor,
                                 final EvalConf evalConf,
                                 final Map<String, InetSocketAddress> executorAddressMap,
                                 final Map<Integer, String> dstTaskIndexTargetExecutorMap,
                                 final byte[] serializedDag,
                                 final OffloadingWorkerFactory offloadingWorkerFactory,
                                 final Map<String, List<String>> taskOutgoingEdges,
                                 final SerializerManager serializerManager,
                                 final ConcurrentLinkedQueue<Object> offloadingEventQueue,
                                 final List<SourceVertexDataFetcher> sourceVertexDataFetchers,
                                 final String taskId,
                                 final List<DataFetcher> availableFetchers,
                                 final List<DataFetcher> pendingFetchers,
                                 final AtomicReference<TaskExecutor.Status> taskStatus,
                                 final AtomicLong prevOffloadStartTime,
                                 final AtomicLong prevOffloadEndTime,
                                 final PersistentConnectionToMasterMap toMaster,
                                 final Collection<OutputWriter> outputWriters,
                                 final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
                                 final ConcurrentMap<Integer, Long> taskTimeMap,
                                 final TaskInputContextMap taskInputContextMap) {
    this.executorId = executorId;
    this.task = task;
    this.taskTimeMap = taskTimeMap;
    this.taskExecutor = taskExecutor;
    this.evalConf = evalConf;
    this.executorAddressMap = executorAddressMap;
    this.dstTaskIndexTargetExecutorMap = dstTaskIndexTargetExecutorMap;
    this.serializedDag = serializedDag;
    this.offloadingWorkerFactory = offloadingWorkerFactory;
    this.taskOutgoingEdges = taskOutgoingEdges;
    this.serializerManager = serializerManager;
    this.offloadingEventQueue = offloadingEventQueue;
    this.sourceVertexDataFetchers = sourceVertexDataFetchers;
    this.taskId = taskId;
    try {
      this.streamingWorkerService = createStreamingWorkerService();
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    this.taskStatus = taskStatus;
    this.prevOffloadEndTime = prevOffloadEndTime;
    this.prevOffloadStartTime = prevOffloadStartTime;
    this.toMaster = toMaster;
    this.outputWriters = outputWriters;
    this.irVertexDag = irVertexDag;
    this.taskInputContextMap = taskInputContextMap;

    logger.scheduleAtFixedRate(() -> {

      LOG.info("Pending offloaded ids at {}: {}", taskId, offloadedDataFetcherMap.keySet());

    }, 2, 2, TimeUnit.SECONDS);
  }

  private StreamingWorkerService createStreamingWorkerService() throws ExecutionException, InterruptedException {
    // build DAG
    final DAG<IRVertex, Edge<IRVertex>> copyDag = SerializationUtils.deserialize(serializedDag);

    copyDag.getVertices().forEach(vertex -> {
      if (vertex instanceof BeamUnboundedSourceVertex) {
        // TODO: we should send unbounded source
        ((BeamUnboundedSourceVertex) vertex).setUnboundedSource(null);
      }
      // this edge can be offloaded
      if (vertex.isSink) {
        vertex.isOffloading = false;
      } else {
        vertex.isOffloading = true;
      }
    });
    final CompletableFuture<ControlMessage.Message> request = requestTaskIndex();

    final StreamingWorkerService streamingWorkerService =
      new StreamingWorkerService(offloadingWorkerFactory,
        new DownstreamOffloadingTransform(
          executorId,
          taskId,
          (int) request.get().getTaskIndexInfoMsg().getTaskIndex(),
          RuntimeIdManager.getIndexFromTaskId(taskId),
          evalConf.samplingJson,
          copyDag,
          taskOutgoingEdges,
          executorAddressMap,
          serializerManager.runtimeEdgeIdToSerializer,
          dstTaskIndexTargetExecutorMap,
          task.getTaskOutgoingEdges()),
        new DownstreamOffloadingSerializer(serializerManager.runtimeEdgeIdToSerializer),
        new StatelessOffloadingEventHandler(offloadingEventQueue, taskTimeMap));

    return streamingWorkerService;
  }

  @Override
  public void handleOffloadingOutput(KafkaOffloadingOutput output) {
    throw new RuntimeException("Unsupported op: " + output);
  }

  @Override
  public void offloadingData(final Object event,
                             final List<String> nextOperatorIds,
                             final long wm,
                             final String edgeId) {
    throw new RuntimeException("No supported operation!!");
  }

  @Override
  public synchronized void handleEndOffloadingEvent() {
    prevOffloadEndTime.set(System.currentTimeMillis());

    if (taskStatus.compareAndSet(TaskExecutor.Status.OFFLOADED, TaskExecutor.Status.DEOFFLOAD_PENDING)) {
      // It means that all tasks are offloaded

      // We will wait for the checkpoint mark of these workers
      // and restart the workers
      for (final OffloadingWorker runningWorker : runningWorkers) {
        LOG.info("Closing running worker {} at {}", runningWorker.getId(), taskId);
        runningWorker.forceClose();
      }
      runningWorkers.clear();


      // Restart contexts
      LOG.info("Restart output writers");
      outputWriters.forEach(OutputWriter::restart);


      taskStatus.set(TaskExecutor.Status.RUNNING);
      taskTimeMap.clear();

    } else if (taskStatus.compareAndSet(TaskExecutor.Status.OFFLOAD_PENDING, TaskExecutor.Status.RUNNING)) {
      taskExecutor.getPrevOffloadEndTime().set(System.currentTimeMillis());
      LOG.info("Get end offloading kafka event: {}", taskStatus);
      // It means that this is not initialized yet
      // just finish this worker!
      for (final KafkaOffloadingRequestEvent event : kafkaOffloadPendingEvents) {
        event.offloadingWorker.forceClose();
        // restart the workers
        // * This is already running... we don't have to restart it
        //LOG.info("Just restart source {} init workers at {}", event.id, taskId);
        //restartDataFetcher(event.sourceVertexDataFetcher, event.checkpointMark, event.id);
      }

      kafkaOffloadPendingEvents.clear();
      taskTimeMap.clear();

      // Restart contexts
      LOG.info("Restart output writers");
      outputWriters.forEach(OutputWriter::restart);


      if (!runningWorkers.isEmpty()) {
        throw new RuntimeException("Offload pending should not have running workers!: " + runningWorkers.size());
      }

    } else {
      throw new RuntimeException("Invalid task status " + taskStatus);
    }
  }

  private CompletableFuture<ControlMessage.Message> requestTaskIndex() {
    return toMaster
      .getMessageSender(MessageEnvironment.SCALEOUT_MESSAGE_LISTENER_ID).request(
        ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(MessageEnvironment.SCALEOUT_MESSAGE_LISTENER_ID)
          .setType(ControlMessage.MessageType.RequestTaskIndex)
          .setRequestTaskIndexMsg(ControlMessage.RequestTaskIndexMessage.newBuilder()
            .setTaskId(taskId)
            .setExecutorId(executorId)
            .build())
          .build());
  }

  @Override
  public synchronized void handleStartOffloadingEvent() {

    prevOffloadStartTime.set(System.currentTimeMillis());

    if (!taskStatus.compareAndSet(TaskExecutor.Status.RUNNING, TaskExecutor.Status.OFFLOAD_PENDING)) {
      LOG.warn("Multiple start request ... just ignore it");
      throw new RuntimeException("Invalid task status: " + taskStatus);
    }

    taskTimeMap.clear();

    if (!kafkaOffloadPendingEvents.isEmpty()) {
      LOG.warn("Task {} received start offloading, but it still offloads sources {}",
        taskId, kafkaOffloadPendingEvents.size());
      // still offloading data fetchers.. skip
      return;
    }

    final OffloadingWorker worker = streamingWorkerService.createStreamWorker();

    kafkaOffloadPendingEvents.add(new KafkaOffloadingRequestEvent(
      worker, sourceId.getAndIncrement(), null));
  }

  public boolean hasPendingStraemingWorkers() {
    return !kafkaOffloadPendingEvents.isEmpty();
  }

  private boolean checkIsAllPendingReady() {
    for (final KafkaOffloadingRequestEvent requestEvent : kafkaOffloadPendingEvents) {
      if (!requestEvent.offloadingWorker.isReady()) {
        return false;
      }
    }
    return true;
  }

  public synchronized void handlePendingStreamingWorkers() {

    if (kafkaOffloadPendingEvents.isEmpty()) {
      LOG.warn("HandlePendingStreamingWorker should be called with hasPendingStreamingWorker");
      return;
    }

    if (checkIsAllPendingReady()) {
      // 1. send message to upstream tasks
      final List<ByteInputContext> byteInputContexts = taskInputContextMap
        .getTaskInputContextMap().get(taskId);

      for (final ByteInputContext byteInputContext : byteInputContexts) {

        final ByteTransferContextSetupMessage pendingMsg =
          new ByteTransferContextSetupMessage(byteInputContext.getRemoteExecutorId(),
            byteInputContext.getContextId().getTransferIndex(),
            byteInputContext.getContextId().getDataDirection(),
            byteInputContext.getContextDescriptor(),
            byteInputContext.getContextId().isPipe(),
            ByteTransferContextSetupMessage.MessageType.PENDING_FOR_SCALEOUT_VM);

        byteInputContext.sendMessage(pendingMsg, (m) -> {
          // ack handler!
          // this guarantees that we received all events from upstream tasks

          // 2. offload downstream tasks
          final KafkaOffloadingRequestEvent event = kafkaOffloadPendingEvents.poll();
          if (!kafkaOffloadPendingEvents.isEmpty()) {
            throw new RuntimeException("We just offload one task!");
          }

          while (!event.offloadingWorker.isReady()) {
            try {
              Thread.sleep(200);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }

          runningWorkers.add(event.offloadingWorker);

          taskExecutor.getPrevOffloadStartTime().set(System.currentTimeMillis());

          // 3. send address to the upstream task
          final String vmAddress =
            event.offloadingWorker.getChannel().remoteAddress().toString().replace("/", "");

          LOG.info("VM Address: {}", vmAddress);

          final ByteTransferContextSetupMessage scaleoutMsg =
          new ByteTransferContextSetupMessage(byteInputContext.getRemoteExecutorId(),
            byteInputContext.getContextId().getTransferIndex(),
            byteInputContext.getContextId().getDataDirection(),
            byteInputContext.getContextDescriptor(),
            byteInputContext.getContextId().isPipe(),
            ByteTransferContextSetupMessage.MessageType.RESUME_AFTER_SCALEOUT_VM,
            vmAddress, taskId);
          byteInputContext.sendMessage(scaleoutMsg, (n) -> {});

          LOG.info("Send scaleout message");

          //byteInputContext.sendMessage();
          //throw new RuntimeException("TODO");
        });
      }
    }
  }
}
