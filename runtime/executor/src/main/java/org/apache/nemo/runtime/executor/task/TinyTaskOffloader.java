package org.apache.nemo.runtime.executor.task;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.kafka.KafkaCheckpointMark;
import org.apache.beam.sdk.io.kafka.KafkaUnboundedSource;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.compiler.frontend.beam.source.BeamUnboundedSourceVertex;
import org.apache.nemo.compiler.frontend.beam.source.UnboundedSourceReadable;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalState;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalTransform;
import org.apache.nemo.compiler.frontend.beam.transform.GBKPartialTransform;
import org.apache.nemo.compiler.frontend.beam.transform.StatefulTransform;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.client.StreamingWorkerService;
import org.apache.nemo.offloading.common.OffloadingSerializer;
import org.apache.nemo.offloading.common.OffloadingWorker;
import org.apache.nemo.offloading.common.OffloadingWorkerFactory;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.executor.TinyTaskOffloadingWorkerManager;
import org.apache.nemo.runtime.executor.TransformContextImpl;
import org.apache.nemo.runtime.executor.common.DataFetcher;
import org.apache.nemo.runtime.executor.common.SourceVertexDataFetcher;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.executor.common.TaskLocationMap;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.datatransfer.OutputWriter;
import org.apache.nemo.runtime.executor.relayserver.RelayServer;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingHeartbeatEvent;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultEvent;
import org.apache.nemo.runtime.lambdaexecutor.StateOutput;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingExecutorSerializer;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingTask;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutput;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingSerializer;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public final class TinyTaskOffloader implements Offloader {
  private static final Logger LOG = LoggerFactory.getLogger(TinyTaskOffloader.class.getName());

  private final byte[] serializedDag;
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
  private final Map<Pair<String, Integer>, String> taskExecutorIdMap;
  private final String executorId;
  private final Task task;

  private final EvalConf evalConf;
  private final PersistentConnectionToMasterMap toMaster;
  private final Collection<OutputWriter> outputWriters;
  final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag;


  private final List<KafkaOffloadingOutput> kafkaOffloadingOutputs = new ArrayList<>();
  private final TaskExecutor taskExecutor;

  private final TinyTaskOffloadingWorkerManager tinyWorkerManager;
  private final SourceVertexDataFetcher sourceVertexDataFetcher;
   private final List<DataFetcher> availableFetchers;
  private final List<DataFetcher> pendingFetchers;
  private final DAG<IRVertex, RuntimeEdge<IRVertex>> copyDag;

  private final List<StageEdge> copyOutgoingEdges;
  private final List<StageEdge> copyIncomingEdges;

  private final List<Future> pendingFutures = new ArrayList<>();

  private final List<DataFetcher> allFetchers = new ArrayList<>();

  private final TaskLocationMap taskLocationMap;

  public TinyTaskOffloader(final String executorId,
                           final Task task,
                           final TaskExecutor taskExecutor,
                           final EvalConf evalConf,
                           final Map<String, InetSocketAddress> executorAddressMap,
                           final Map<Pair<String, Integer>, String> taskExecutorIdMap,
                           final byte[] serializedDag,
                           final List<StageEdge> copyOutgoingEdges,
                           final List<StageEdge> copyIncomingEdges,
                           final TinyTaskOffloadingWorkerManager tinyWorkerManager,
                           final Map<String, List<String>> taskOutgoingEdges,
                           final SerializerManager serializerManager,
                           final ConcurrentLinkedQueue<Object> offloadingEventQueue,
                           final List<SourceVertexDataFetcher> sourceVertexDataFetchers,
                           final String taskId,
                           final List<DataFetcher> availableFetchers,
                           final List<DataFetcher> pendingFetchers,
                           final SourceVertexDataFetcher sourceDataFetcher,
                           final AtomicReference<TaskExecutor.Status> taskStatus,
                           final AtomicLong prevOffloadStartTime,
                           final AtomicLong prevOffloadEndTime,
                           final PersistentConnectionToMasterMap toMaster,
                           final Collection<OutputWriter> outputWriters,
                           final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
                           final RelayServer relayServer,
                           final TaskLocationMap taskLocationMap) {
    this.executorId = executorId;
    this.task = task;
    this.taskExecutor = taskExecutor;
    this.copyOutgoingEdges = copyOutgoingEdges;
    this.copyIncomingEdges = copyIncomingEdges;
    this.evalConf = evalConf;
    this.executorAddressMap = executorAddressMap;
    this.taskExecutorIdMap = taskExecutorIdMap;
    this.serializedDag = serializedDag;
    this.copyDag = SerializationUtils.deserialize(serializedDag);
    this.tinyWorkerManager = tinyWorkerManager;
    this.taskOutgoingEdges = taskOutgoingEdges;
    this.availableFetchers = availableFetchers;
    this.pendingFetchers = pendingFetchers;
    this.allFetchers.addAll(availableFetchers);
    this.allFetchers.addAll(pendingFetchers);
    this.serializerManager = serializerManager;
    this.sourceVertexDataFetcher = sourceDataFetcher;

    this.offloadingEventQueue = offloadingEventQueue;
    this.sourceVertexDataFetchers = sourceVertexDataFetchers;
    this.taskId = taskId;
    this.taskStatus = taskStatus;
    this.prevOffloadEndTime = prevOffloadEndTime;
    this.prevOffloadStartTime = prevOffloadStartTime;
    this.toMaster = toMaster;
    this.outputWriters = outputWriters;
    this.irVertexDag = irVertexDag;

    this.taskLocationMap = taskLocationMap;

    logger.scheduleAtFixedRate(() -> {

      LOG.info("Pending offloaded ids at {}: {}", taskId, offloadedDataFetcherMap.keySet());

    }, 2, 2, TimeUnit.SECONDS);
  }

  @Override
  public synchronized void handleOffloadingOutput(final KafkaOffloadingOutput output) {

    //여기서 task offload 되엇다는 message handle하기
      // Sink redirect

    // Source redirect
    //  dataFetcher.redirectTo(...);

    if (!taskStatus.get().equals(TaskExecutor.Status.DEOFFLOAD_PENDING)) {
      throw new RuntimeException("Invalid status: " + taskStatus);
    }

    final int id = output.id;
    final KafkaCheckpointMark checkpointMark = (KafkaCheckpointMark) output.checkpointMark;
    LOG.info("Receive checkpoint mark for source {} in VM: {} at task {}, sourceVertices: {}"
      , id, checkpointMark, taskId, sourceVertexDataFetchers.size());

    LOG.info("Source vertex datafetchers: {}", sourceVertexDataFetchers);

    final UnboundedSourceReadable readable = (UnboundedSourceReadable) sourceVertexDataFetcher.getReadable();
    final UnboundedSource oSource = readable.getUnboundedSource();

    LOG.info("Prepare source !!! {}", oSource);

    final UnboundedSourceReadable newReadable =
      new UnboundedSourceReadable(oSource, null, checkpointMark);

    sourceVertexDataFetcher.setReadable(newReadable);

    // set state
     if (output.stateMap != null) {
      for (final String key : output.stateMap.keySet()) {
        LOG.info("Reset state for operator {}", key);
        final OperatorVertex statefulOp = getStateTransformVertex(key);
        final StatefulTransform transform = (StatefulTransform) statefulOp.getTransform();
        transform.setState(output.stateMap.get(key));
      }
    }

    availableFetchers.add(sourceVertexDataFetcher);

    kafkaOffloadingOutputs.clear();

    taskStatus.set(TaskExecutor.Status.RUNNING);
  }

  @Override
  public void handleStateOutput(StateOutput output) {
    if (!taskStatus.get().equals(TaskExecutor.Status.DEOFFLOAD_PENDING)) {
      throw new RuntimeException("Invalid status: " + taskStatus);
    }

    if (output.stateMap != null) {
      for (final String key : output.stateMap.keySet()) {
        LOG.info("Set state for operator {}", key);
        final OperatorVertex statefulOp = getStateTransformVertex(key);
        final StatefulTransform transform = (StatefulTransform) statefulOp.getTransform();
        transform.setState(output.stateMap.get(key));
      }
    }

    // restart input context!
    LOG.info("Restart input context  at {}!!!", output.taskId);

    // Source stop!!
    // Source stop!!
    // Source stop!!
    for (final DataFetcher dataFetcher : allFetchers) {
      dataFetcher.restart();
    }

    availableFetchers.addAll(allFetchers);

    taskStatus.set(TaskExecutor.Status.RUNNING);
  }

  @Override
  public void offloadingData(Object event, List<String> nextOperatorIds, long wm,
                             final String edgeId) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public synchronized void handleEndOffloadingEvent() {

    prevOffloadEndTime.set(System.currentTimeMillis());

    if (taskStatus.compareAndSet(TaskExecutor.Status.OFFLOADED, TaskExecutor.Status.DEOFFLOAD_PENDING)) {
      tinyWorkerManager.deleteTask(taskId);

    } else {
      throw new RuntimeException("Unsupported status: " + taskStatus);
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

    // TODO: 1) Remove available and pending fetchers!!
    // Stop sources and output emission!!

    // Source stop!!
    // Source stop!!
    // Source stop!!
    for (final DataFetcher dataFetcher : allFetchers) {
      pendingFutures.add(dataFetcher.stop());
    }
    // Source stop!!
    // Source stop!!
    taskStatus.compareAndSet(TaskExecutor.Status.RUNNING, TaskExecutor.Status.OFFLOAD_PENDING);

    LOG.info("Waiting for source stop futures...");
  }

  @Override
  public boolean hasPendingStraemingWorkers() {
    return !pendingFutures.isEmpty() && checkIsAllPendingReady() && availableFetchers.isEmpty();
  }

  private boolean checkIsAllPendingReady() {
    for (final Future future : pendingFutures) {
      if (!future.isDone()) {
        return false;
      }
    }
    return true;
  }

  private OperatorVertex getStateTransformVertex(final String opId) {
    for (final IRVertex vertex : irVertexDag.getVertices()) {
      if (vertex.getId().equals(opId)) {
        return (OperatorVertex) vertex;
      }
    }

    throw new RuntimeException("Cannot find stateful transform");
  }

  private Pair<Map<String, GBKFinalState>, Map<String, Coder<GBKFinalState>>> getStateAndCoderMap() {
    final Map<String, GBKFinalState> stateMap = new HashMap<>();
        final Map<String, Coder<GBKFinalState>> coderMap = new HashMap<>();
    for (final IRVertex vertex : irVertexDag.getVertices()) {
      if (vertex instanceof OperatorVertex) {
        final Transform transform = ((OperatorVertex) vertex).getTransform();
        if (transform instanceof StatefulTransform) {
          final StatefulTransform finalTransform = (StatefulTransform) transform;
          stateMap.put(vertex.getId(), (GBKFinalState) finalTransform.getState());
          coderMap.put(vertex.getId(), finalTransform.getStateCoder());
        }
      }
    }

    return Pair.of(stateMap, coderMap);
  }

  @Override
  public synchronized void handlePendingStreamingWorkers() {

    //LOG.info("Available: {}, Pending: {}, task {}", availableFetchers, pendingFetchers, taskId);

    availableFetchers.clear();
    pendingFetchers.clear();


    pendingFutures.clear();
    LOG.info("End of waiting source stop futures...");

    // Sink stop!!
    // Sink stop!!
    // Sink stop!!

    LOG.info("Close current output contexts");
    outputWriters.forEach(writer -> {
      LOG.info("Stopping writer {}", writer);
      writer.stop();
    });
    // Sink stop!!
    // Sink stop!!
    // Sink stop!!
    final OffloadingTask offloadingTask;
    final Pair<Map<String, GBKFinalState>, Map<String, Coder<GBKFinalState>>>
      stateAndCoderMap = getStateAndCoderMap();

    final Map<String, GBKFinalState> stateMap = stateAndCoderMap.left();
    final Map<String, Coder<GBKFinalState>> coderMap = stateAndCoderMap.right();


    if (sourceVertexDataFetcher != null) {
      final UnboundedSourceReadable readable = (UnboundedSourceReadable) sourceVertexDataFetcher.getReadable();
      final KafkaCheckpointMark checkpointMark = (KafkaCheckpointMark) readable.getReader().getCheckpointMark();
      final KafkaUnboundedSource unboundedSource = (KafkaUnboundedSource) readable.getUnboundedSource();

      LOG.info("Get unbounded source: {}", unboundedSource);

      final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder = unboundedSource.getCheckpointMarkCoder();

      LOG.info("Send checkpoint mark at task {}: {}", taskId, checkpointMark);
      LOG.info("Sending location map at {}: {}", taskId, taskLocationMap.locationMap);

      offloadingTask = new OffloadingTask(
        executorId,
        taskId,
        RuntimeIdManager.getIndexFromTaskId(taskId),
        evalConf.samplingJson,
        copyDag,
        taskOutgoingEdges,
        copyOutgoingEdges,
        copyIncomingEdges,
        checkpointMark,
        checkpointMarkCoder,
        unboundedSource,
        stateMap,
        taskLocationMap.locationMap);

      final OffloadingSerializer serializer = new OffloadingExecutorSerializer(coderMap);

      tinyWorkerManager.sendTask(offloadingTask, taskExecutor, serializer, coderMap);
    } else {
      offloadingTask = new OffloadingTask(
        executorId,
        taskId,
        RuntimeIdManager.getIndexFromTaskId(taskId),
        evalConf.samplingJson,
        copyDag,
        taskOutgoingEdges,
        copyOutgoingEdges,
        copyIncomingEdges,
        null,
        null,
        null,
        stateMap,
        taskLocationMap.locationMap);


      final OffloadingSerializer serializer = new OffloadingExecutorSerializer(coderMap);
      tinyWorkerManager.sendTask(offloadingTask, taskExecutor, serializer, coderMap);
    }

    prevOffloadStartTime.set(System.currentTimeMillis());

    if (!taskStatus.compareAndSet(TaskExecutor.Status.OFFLOAD_PENDING, TaskExecutor.Status.OFFLOADED)) {
      LOG.warn("Multiple start request ... just ignore it");
      throw new RuntimeException("Invalid task status: " + taskStatus);
    }
  }
}
