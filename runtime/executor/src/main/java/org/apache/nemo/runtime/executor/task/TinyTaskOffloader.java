package org.apache.nemo.runtime.executor.task;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.kafka.KafkaCheckpointMark;
import org.apache.beam.sdk.io.kafka.KafkaUnboundedSource;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.NemoTriple;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.compiler.frontend.beam.source.UnboundedSourceReadable;
import org.apache.nemo.compiler.frontend.beam.transform.GBKFinalState;
import org.apache.nemo.compiler.frontend.beam.transform.StatefulTransform;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.common.OffloadingSerializer;
import org.apache.nemo.offloading.common.OffloadingWorker;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.executor.GlobalOffloadDone;
import org.apache.nemo.runtime.executor.RemainingOffloadTasks;
import org.apache.nemo.runtime.executor.TinyTaskOffloadingWorkerManager;
import org.apache.nemo.runtime.executor.TinyTaskWorker;
import org.apache.nemo.runtime.executor.common.DataFetcher;
import org.apache.nemo.runtime.executor.common.SourceVertexDataFetcher;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.executor.common.TaskLocationMap;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.datatransfer.OutputWriter;
import org.apache.nemo.runtime.executor.relayserver.RelayServer;
import org.apache.nemo.runtime.lambdaexecutor.ReadyTask;
import org.apache.nemo.runtime.lambdaexecutor.StateOutput;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingExecutorSerializer;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingTask;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
  private final Map<NemoTriple<String, Integer, Boolean>, String> taskExecutorIdMap;
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

  private final List<Future> inputStopPendingFutures = new ArrayList<>();
  private final List<Future> outputStopPendingFutures = new ArrayList<>();

  private final Set<DataFetcher> allFetchers = new HashSet<>();

  private final TaskLocationMap taskLocationMap;

  private TaskExecutor.PendingState pendingStatus = TaskExecutor.PendingState.WORKER_PENDING;
  private TinyTaskWorker tinyTaskWorker;

  private final RemainingOffloadTasks remainingOffloadTasks = RemainingOffloadTasks.getInstance();
  private final GlobalOffloadDone globalOffloadDone = GlobalOffloadDone.getInstance();

  public TinyTaskOffloader(final String executorId,
                           final Task task,
                           final TaskExecutor taskExecutor,
                           final EvalConf evalConf,
                           final Map<String, InetSocketAddress> executorAddressMap,
                           final Map<NemoTriple<String, Integer, Boolean>, String> taskExecutorIdMap,
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

    /*
    logger.scheduleAtFixedRate(() -> {

      LOG.info("Pending offloaded ids at {}: {}", taskId, offloadedDataFetcherMap.keySet());

    }, 2, 2, TimeUnit.SECONDS);
    */
  }

  @Override
  public TaskExecutor.PendingState getPendingStatus() {
    return pendingStatus;
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

    outputWriters.forEach(writer -> {
      LOG.info("Restarting writer {}", writer);
      writer.restart(taskId);
    });

    for (final DataFetcher dataFetcher : allFetchers) {
      dataFetcher.restart();
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

    outputWriters.forEach(writer -> {
      LOG.info("Restarting writer {}", writer);
      writer.restart(taskId);
    });

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
      if (tinyWorkerManager.deleteTask(taskId)) {
        // removed immediately !!
        // restart
        LOG.info("{} is not offloaded.. just restart it", taskId);
        outputWriters.forEach(writer -> {
          LOG.info("Restarting writer {}", writer);
          writer.restart(taskId);
        });

        for (final DataFetcher dataFetcher : allFetchers) {
          dataFetcher.restart();
        }

        availableFetchers.addAll(allFetchers);
        taskStatus.set(TaskExecutor.Status.RUNNING);
      }

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
  public synchronized void handleStartOffloadingEvent(final TinyTaskWorker worker) {

    // TODO: 1) Remove available and pending fetchers!!
    // Stop sources and output emission!!

    // Prepare task offloading
    //final OffloadingSerializer serializer = new OffloadingExecutorSerializer();
    //tinyTaskWorker = tinyWorkerManager.prepareSendTask(serializer);

    tinyTaskWorker = worker;

    LOG.info("Waiting worker {} for {}", tinyTaskWorker, taskId);

    // Source stop!!
    for (final DataFetcher dataFetcher : allFetchers) {
      inputStopPendingFutures.add(dataFetcher.stop(taskId));
    }

    LOG.info("Waiting for source stop futures in {}", taskId);

    taskStatus.compareAndSet(TaskExecutor.Status.RUNNING, TaskExecutor.Status.OFFLOAD_PENDING);
    pendingStatus = TaskExecutor.PendingState.WORKER_PENDING;
  }

  @Override
  public boolean hasPendingStraemingWorkers() {
    if (!taskStatus.get().equals(TaskExecutor.Status.OFFLOAD_PENDING)) {
      return false;
    }

    switch (pendingStatus) {
      case WORKER_PENDING: {
        if (tinyTaskWorker.isReady()) {
          LOG.info("Worker is in ready {} for {}", tinyTaskWorker, taskId);
          pendingStatus = TaskExecutor.PendingState.INPUT_PENDING;
        } else {
          //LOG.info("Worker not ready {} for {}", tinyTaskWorker, taskId);
          break;
        }
      }
      case INPUT_PENDING: {
        if (checkIsAllInputPendingReady()) {
          inputStopPendingFutures.clear();
          LOG.info("Input pending done {}", taskId);
          if (availableFetchers.isEmpty()) {
            LOG.info("End of waiting source stop futures...");
            LOG.info("Close current output contexts in {}", taskId);
            startOutputPending();
            pendingStatus = TaskExecutor.PendingState.OUTPUT_PENDING;
          }
        } else {
          //LOG.info("Input pending not done {}", taskId);
          break;
        }
      }
      case OUTPUT_PENDING: {
        if (checkIsAllOutputPendingReady()) {
          LOG.info("Output stop done for {}", taskId);
          outputStopPendingFutures.clear();
          pendingStatus = TaskExecutor.PendingState.OTHER_TASK_WAITING;
          remainingOffloadTasks.decrementAndGet();


          // TODO: send it to the lambda!!
          sendTask();
          return true;
        } else {
          break;
        }
      }
      case OTHER_TASK_WAITING: {
        if (globalOffloadDone.getBoolean().get()) {
          return true;
        } else {
          LOG.info("Waiting other offload tasks... {}/{}", remainingOffloadTasks.getRemainingCnt(),
            globalOffloadDone.getBoolean());
          break;
        }
      }
    }

    return false;
  }

  private void sendTask() {
    availableFetchers.clear();
    pendingFetchers.clear();

    final OffloadingTask offloadingTask;
    final Pair<Map<String, GBKFinalState>, Map<String, Coder<GBKFinalState>>>
      stateAndCoderMap = getStateAndCoderMap();

    final Map<String, GBKFinalState> stateMap = stateAndCoderMap.left();
    final Map<String, Coder<GBKFinalState>> coderMap = stateAndCoderMap.right();


    if (sourceVertexDataFetcher != null) {
      final UnboundedSourceReadable readable = (UnboundedSourceReadable) sourceVertexDataFetcher.getReadable();
      final KafkaCheckpointMark checkpointMark = (KafkaCheckpointMark) readable.getReader().getCheckpointMark();
      final KafkaUnboundedSource unboundedSource = (KafkaUnboundedSource) readable.getUnboundedSource();

      final long prevWatermarkTimestamp = sourceVertexDataFetcher.getPrevWatermarkTimestamp();

      LOG.info("Get unbounded source: {}", unboundedSource);

      final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder = unboundedSource.getCheckpointMarkCoder();

      //LOG.info("Send checkpoint mark at task {}: {}", taskId, checkpointMark);
      //LOG.info("Sending location map at {}: {}", taskId, taskLocationMap.locationMap);

      taskExecutor.setOffloadedTaskTime(0);

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
        prevWatermarkTimestamp,
        unboundedSource,
        stateMap,
        coderMap);

      final OffloadingSerializer serializer = new OffloadingExecutorSerializer();

      tinyWorkerManager.sendTask(offloadingTask, taskExecutor, tinyTaskWorker);
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
        -1,
        null,
        stateMap,
        coderMap);

      tinyWorkerManager.sendTask(offloadingTask, taskExecutor, tinyTaskWorker);
    }

    LOG.info("Send actual task {}", taskId);
  }

  private boolean checkIsAllInputPendingReady() {
    for (final Future future : inputStopPendingFutures) {
      if (!future.isDone()) {
        return false;
      }
    }
    return true;
  }

  private void startOutputPending() {
    outputWriters.forEach(writer -> {
      LOG.info("Stopping writer {}", writer);
      outputStopPendingFutures.add(writer.stop(taskId));
    });
  }

  private boolean checkIsAllOutputPendingReady() {
    for (final Future future : outputStopPendingFutures) {
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
    // Send ready signal and other data
    tinyWorkerManager.sendReadyTask(new ReadyTask(taskId, taskLocationMap.locationMap), tinyTaskWorker);

    prevOffloadStartTime.set(System.currentTimeMillis());

    if (!taskStatus.compareAndSet(TaskExecutor.Status.OFFLOAD_PENDING, TaskExecutor.Status.OFFLOADED)) {
      LOG.warn("Multiple start request ... just ignore it");
      throw new RuntimeException("Invalid task status: " + taskStatus);
    }
  }
}
