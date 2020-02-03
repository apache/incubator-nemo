package org.apache.nemo.runtime.executor.task;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.kafka.KafkaCheckpointMark;
import org.apache.beam.sdk.io.kafka.KafkaUnboundedSource;
import org.apache.commons.lang3.SerializationUtils;
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
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.executor.*;
import org.apache.nemo.runtime.executor.common.DataFetcher;
import org.apache.nemo.runtime.executor.common.ExecutorThread;
import org.apache.nemo.runtime.executor.common.SourceVertexDataFetcher;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.common.TaskLocationMap;
import org.apache.nemo.runtime.executor.datatransfer.OutputWriter;
import org.apache.nemo.runtime.lambdaexecutor.ReadyTask;
import org.apache.nemo.runtime.lambdaexecutor.StateOutput;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingTask;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class TinyTaskOffloader implements Offloader {
  public static final Logger LOG = LoggerFactory.getLogger(TinyTaskOffloader.class.getName());

  public final Map<String, List<String>> taskOutgoingEdges;
  public final List<SourceVertexDataFetcher> sourceVertexDataFetchers;
  public final String taskId;

  public final AtomicReference<TaskExecutor.Status> taskStatus;

  public final ScheduledExecutorService logger = Executors.newSingleThreadScheduledExecutor();

  public final AtomicLong prevOffloadStartTime;
  public final AtomicLong prevOffloadEndTime;

  public final String executorId;

  public final EvalConf evalConf;
  public final PersistentConnectionToMasterMap toMaster;
  public final Collection<OutputWriter> outputWriters;
  public final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag;

  public final List<KafkaOffloadingOutput> kafkaOffloadingOutputs = new ArrayList<>();
  public final TaskExecutor taskExecutor;

  public final TinyTaskOffloadingWorkerManager tinyWorkerManager;
  public final SourceVertexDataFetcher sourceVertexDataFetcher;
  public final DAG<IRVertex, RuntimeEdge<IRVertex>> copyDag;

  public final List<StageEdge> copyOutgoingEdges;
  public final List<StageEdge> copyIncomingEdges;

  final List<Future> inputStopPendingFutures = new ArrayList<>();
  public final List<Future> outputStopPendingFutures = new ArrayList<>();

  public final Set<DataFetcher> allFetchers = new HashSet<>();

  public final TaskLocationMap taskLocationMap;

  public TaskExecutor.PendingState pendingStatus = TaskExecutor.PendingState.WORKER_PENDING;
  public TinyTaskWorker tinyTaskWorker;

  public final RemainingOffloadTasks remainingOffloadTasks = RemainingOffloadTasks.getInstance();

  public final ExecutorThread executorThread;
  public final ScheduledExecutorService scheduledExecutorService;
  public final ScalingOutCounter scalingOutCounter;
  public final Map<String, String> taskExecutorIdMap;

  public TinyTaskOffloader(final String executorId,
                           final TaskExecutor taskExecutor,
                           final EvalConf evalConf,
                           final byte[] serializedDag,
                           final List<StageEdge> copyOutgoingEdges,
                           final List<StageEdge> copyIncomingEdges,
                           final TinyTaskOffloadingWorkerManager tinyWorkerManager,
                           final Map<String, List<String>> taskOutgoingEdges,
                           final List<SourceVertexDataFetcher> sourceVertexDataFetchers,
                           final String taskId,
                           final SourceVertexDataFetcher sourceDataFetcher,
                           final AtomicReference<TaskExecutor.Status> taskStatus,
                           final AtomicLong prevOffloadStartTime,
                           final AtomicLong prevOffloadEndTime,
                           final PersistentConnectionToMasterMap toMaster,
                           final Collection<OutputWriter> outputWriters,
                           final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
                           final TaskLocationMap taskLocationMap,
                           final ExecutorThread executorThread,
                           final List<DataFetcher> fetchers,
                           final ScalingOutCounter scalingOutCounter,
                           final Map<String, String> taskExecutorIdMap) {
    this.executorThread = executorThread;
    this.scheduledExecutorService = executorThread.scheduledExecutorService;
    this.executorId = executorId;
    this.taskExecutor = taskExecutor;
    this.copyOutgoingEdges = copyOutgoingEdges;
    this.copyIncomingEdges = copyIncomingEdges;
    this.evalConf = evalConf;
    this.copyDag = SerializationUtils.deserialize(serializedDag);
    this.tinyWorkerManager = tinyWorkerManager;
    this.taskOutgoingEdges = taskOutgoingEdges;
    this.sourceVertexDataFetcher = sourceDataFetcher;

    this.scalingOutCounter = scalingOutCounter;
    this.taskExecutorIdMap = taskExecutorIdMap;

    this.sourceVertexDataFetchers = sourceVertexDataFetchers;
    this.taskId = taskId;
    this.taskStatus = taskStatus;
    this.prevOffloadEndTime = prevOffloadEndTime;
    this.prevOffloadStartTime = prevOffloadStartTime;
    this.toMaster = toMaster;
    this.outputWriters = outputWriters;
    this.irVertexDag = irVertexDag;

    this.taskLocationMap = taskLocationMap;

    this.allFetchers.addAll(fetchers);

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
      throw new RuntimeException("Invalid status: " + taskStatus + " for task " + output.taskId);
    }

    if (output.moveToVMScaling) {

      LOG.info("Receiving task for moving to VM scaling 111 {}, status {}",
        output.taskId, taskStatus);

    } else {
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

      kafkaOffloadingOutputs.clear();

      LOG.info("Set statues running for task {} 111", taskId);
      taskStatus.set(TaskExecutor.Status.RUNNING);
    }
  }

  @Override
  public synchronized void handleStateOutput(StateOutput output) {
    if (!taskStatus.get().equals(TaskExecutor.Status.DEOFFLOAD_PENDING)) {
      throw new RuntimeException("Invalid status: " + taskStatus + " for task " + output.taskId);
    }

    if (output.moveToVmScaling) {
      LOG.info("Receiving task for moving to VM scaling 222 {}, status {}", output.taskId,
        taskStatus);

    } else {
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

      LOG.info("Set statues running for task {} 112", taskId);
      taskStatus.set(TaskExecutor.Status.RUNNING);
    }
  }

  @Override
  public void offloadingData(Object event, List<String> nextOperatorIds, long wm,
                             final String edgeId) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public synchronized void handleEndOffloadingEvent(final boolean mvToVmScaling) {

    prevOffloadEndTime.set(System.currentTimeMillis());

    LOG.info("Handle end offloading event for {}, status {}",
      taskId, taskStatus);

    if (taskStatus.compareAndSet(TaskExecutor.Status.OFFLOADED, TaskExecutor.Status.DEOFFLOAD_PENDING)) {
      if (tinyWorkerManager.deleteTask(taskId, mvToVmScaling)) {
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
        LOG.info("Set statues running for task {} 333", taskId);
        taskStatus.set(TaskExecutor.Status.RUNNING);
      }

    } else {
      throw new RuntimeException("Invalid status: " + taskStatus + " for task " + taskId);
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


    taskStatus.compareAndSet(TaskExecutor.Status.RUNNING, TaskExecutor.Status.OFFLOAD_PENDING);
    pendingStatus = TaskExecutor.PendingState.WORKER_PENDING;

    LOG.info("Scheduling worker pending check {}", taskId);
    scheduledExecutorService.schedule(this::scheduleWorkerPendingCheck, 100, TimeUnit.MILLISECONDS);
  }

  // 1.
  // 2는 taskIsReady
  private void scheduleWorkerPendingCheck() {
    if (tinyTaskWorker.isReady()) {
      LOG.info("Worker is in ready {} for {}", tinyTaskWorker, taskId);
      sendTask();
      LOG.info("Waiting for task {} ready...", taskId);
      pendingStatus = TaskExecutor.PendingState.TASK_READY_PENDING;
    } else {
      scheduledExecutorService.schedule(this::scheduleWorkerPendingCheck, 100, TimeUnit.MILLISECONDS);
    }
  }

  private boolean hasDataInFetchers() {
    for (final DataFetcher fetcher : allFetchers) {
      if (fetcher.hasData()) {
        return true;
      }
    }

    return false;
  }


  // 2. 얘는 offloading done 받으면 trigger됨
  @Override
  public void callTaskOffloadingDone() {
    if (pendingStatus != TaskExecutor.PendingState.TASK_READY_PENDING) {
      throw new RuntimeException("Task is not ready pending but " + pendingStatus);
    }

    executorThread.queue.add(() -> {
      if (tinyTaskWorker.isReady()) {
        // Source stop!!
        for (final DataFetcher dataFetcher : allFetchers) {
          inputStopPendingFutures.add(dataFetcher.stop(taskId));
        }

        LOG.info("Waiting for source stop futures in {}", taskId);
        pendingStatus = TaskExecutor.PendingState.INPUT_PENDING;

        scheduledExecutorService.schedule(this::schedulePendingCheck, 100, TimeUnit.MILLISECONDS);
      } else {
        throw new RuntimeException("Worker should be ready... " + taskId);
      }
    });
  }

  // 3. taskIsReady가 trigger함
  private void schedulePendingCheck() {
    executorThread.queue.add(() -> {
      if (hasPendingStraemingWorkers()) {
        handlePendingStreamingWorkers();
      } else {
        scheduledExecutorService.schedule(this::schedulePendingCheck, 100, TimeUnit.MILLISECONDS);
      }
    });
  }

  // 4. 여기서 state check
  private boolean hasPendingStraemingWorkers() {
    if (!taskStatus.get().equals(TaskExecutor.Status.OFFLOAD_PENDING)) {
      return false;
    }

    switch (pendingStatus) {
      case INPUT_PENDING: {
        if (checkIsAllInputPendingReady()) {
          inputStopPendingFutures.clear();
          LOG.info("Input pending done {}", taskId);
          if (!hasDataInFetchers()) {
            LOG.info("End of waiting source stop futures...");
            LOG.info("Close current output contexts in {}", taskId);
            startOutputPending();
            pendingStatus = TaskExecutor.PendingState.OUTPUT_PENDING;
          } else {
            break;
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
          //remainingOffloadTasks.decrementAndGet();
          pendingStatus = TaskExecutor.PendingState.OTHER_TASK_WAITING;
        } else {
          break;
        }
      }
      case OTHER_TASK_WAITING: {
        return true;
        /*
        if (GlobalOffloadDone.getInstance().getBoolean().get()) {
          LOG.info("global offloading done {}", taskId);
          return true;
        } else {
          break;
        }
        */
      }
    }

    return false;
  }

  // 5.
  private synchronized void handlePendingStreamingWorkers() {
    // Send ready signal and other data
    sendReadyTask();
    prevOffloadStartTime.set(System.currentTimeMillis());

    if (!taskStatus.compareAndSet(TaskExecutor.Status.OFFLOAD_PENDING, TaskExecutor.Status.OFFLOADED)) {
      LOG.warn("Multiple start request ... just ignore it");
      throw new RuntimeException("Invalid task status: " + taskStatus);
    }
  }

  private void sendReadyTask() {
    // Send ready signal and other data
    // offloading done message 받은다음에 ready task 보내야함.

    final Pair<Map<String, GBKFinalState>, Map<String, Coder<GBKFinalState>>>
      stateAndCoderMap = getStateAndCoderMap();

    final Map<String, GBKFinalState> stateMap = stateAndCoderMap.left();
    final Map<String, Coder<GBKFinalState>> coderMap = stateAndCoderMap.right();

    final ReadyTask readyTask;

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

      readyTask = new ReadyTask(
        taskId,
        taskLocationMap.locationMap,
        checkpointMark,
        checkpointMarkCoder,
        prevWatermarkTimestamp,
        unboundedSource,
        stateMap,
        coderMap,
        taskExecutorIdMap);
    } else {
      readyTask = new ReadyTask(
        taskId,
        taskLocationMap.locationMap,
        null,
        null,
        -1,
        null,
        stateMap,
        coderMap,
        taskExecutorIdMap);
    }


    LOG.info("Send ready task {}", taskId);

    tinyWorkerManager.sendReadyTask(readyTask, tinyTaskWorker);

    scalingOutCounter.counter.getAndDecrement();
  }

  private void sendTask() {

    final OffloadingTask offloadingTask = new OffloadingTask(
      executorId,
      taskId,
      RuntimeIdManager.getIndexFromTaskId(taskId),
      evalConf.samplingJson,
      copyDag,
      taskOutgoingEdges,
      copyOutgoingEdges,
      copyIncomingEdges);
    tinyWorkerManager.sendTask(offloadingTask, taskExecutor, tinyTaskWorker);

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
      LOG.info("Stopping writer {} / {}", writer, taskId);
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

}
