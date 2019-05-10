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
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.compiler.frontend.beam.source.BeamUnboundedSourceVertex;
import org.apache.nemo.compiler.frontend.beam.source.UnboundedSourceReadable;
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
import org.apache.nemo.runtime.executor.TinyTaskOffloadingWorkerManager;
import org.apache.nemo.runtime.executor.TransformContextImpl;
import org.apache.nemo.runtime.executor.common.DataFetcher;
import org.apache.nemo.runtime.executor.common.SourceVertexDataFetcher;
import org.apache.nemo.runtime.executor.common.TaskExecutor;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.datatransfer.OutputWriter;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingHeartbeatEvent;
import org.apache.nemo.runtime.lambdaexecutor.OffloadingResultEvent;
import org.apache.nemo.runtime.lambdaexecutor.general.OffloadingTask;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutput;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingSerializer;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
  private final Map<Pair<RuntimeEdge, Integer>, String> taskExecutorIdMap;
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

  public TinyTaskOffloader(final String executorId,
                           final Task task,
                           final TaskExecutor taskExecutor,
                           final EvalConf evalConf,
                           final Map<String, InetSocketAddress> executorAddressMap,
                           final Map<Pair<RuntimeEdge, Integer>, String> taskExecutorIdMap,
                           final byte[] serializedDag,
                           final TinyTaskOffloadingWorkerManager tinyWorkerManager,
                           final Map<String, List<String>> taskOutgoingEdges,
                           final SerializerManager serializerManager,
                           final ConcurrentLinkedQueue<Object> offloadingEventQueue,
                           final List<SourceVertexDataFetcher> sourceVertexDataFetchers,
                           final String taskId,
                           final SourceVertexDataFetcher sourceDataFetcher,
                           final AtomicReference<TaskExecutor.Status> taskStatus,
                           final AtomicLong prevOffloadStartTime,
                           final AtomicLong prevOffloadEndTime,
                           final PersistentConnectionToMasterMap toMaster,
                           final Collection<OutputWriter> outputWriters,
                           final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag) {
    this.executorId = executorId;
    this.task = task;
    this.taskExecutor = taskExecutor;
    this.evalConf = evalConf;
    this.executorAddressMap = executorAddressMap;
    this.taskExecutorIdMap = taskExecutorIdMap;
    this.serializedDag = serializedDag;
    this.tinyWorkerManager = tinyWorkerManager;
    this.taskOutgoingEdges = taskOutgoingEdges;
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

    logger.scheduleAtFixedRate(() -> {

      LOG.info("Pending offloaded ids at {}: {}", taskId, offloadedDataFetcherMap.keySet());

    }, 2, 2, TimeUnit.SECONDS);
  }

  public static KafkaCheckpointMark createCheckpointMarkForSource(
    final KafkaUnboundedSource kafkaUnboundedSource,
    final KafkaCheckpointMark checkpointMark) {

    if (kafkaUnboundedSource.getTopicPartitions().size() > 1) {
      throw new RuntimeException("Kafka has > 1 partitions...");
    }

    final TopicPartition topicPartition = (TopicPartition)
      kafkaUnboundedSource.getTopicPartitions().get(0);

    for (final KafkaCheckpointMark.PartitionMark partitionMark : checkpointMark.getPartitions()) {
      if (partitionMark.getPartition() == topicPartition.partition()) {
        return new KafkaCheckpointMark(Collections.singletonList(
          partitionMark), Optional.empty());
      }
    }

    throw new RuntimeException("Cannot find partitionMark " + topicPartition);
  }

  @Override
  public synchronized void handleOffloadingOutput(final KafkaOffloadingOutput output) {
    // handling checkpoint mark to resume the kafka source reading
    // Serverless -> VM
    // we start to read kafka events again
    final int id = output.id;
    final KafkaCheckpointMark checkpointMark = (KafkaCheckpointMark) output.checkpointMark;
    LOG.info("Receive checkpoint mark for source {} in VM: {}", id, checkpointMark);

    if (!offloadedDataFetcherMap.containsKey(id)) {
      throw new RuntimeException("Source " + id + " is not offloaded yet!");
    }

    kafkaOffloadingOutputs.add(output);

    offloadedDataFetcherMap.remove(id);

    if (offloadedDataFetcherMap.isEmpty()) {
      // It means that offloading finished
      taskExecutor.getPrevOffloadEndTime().set(System.currentTimeMillis());

      if (!taskStatus.compareAndSet(TaskExecutor.Status.DEOFFLOAD_PENDING, TaskExecutor.Status.RUNNING)) {
        throw new RuntimeException("Invalid task status: " + taskStatus);
      }

      taskTimeMap.clear();


      // Restart contexts
      LOG.info("Restart output writers");
      outputWriters.forEach(OutputWriter::restart);

      LOG.info("Merge {} sources into one", kafkaOffloadingOutputs.size());
      // TODO: merge sources!!
      // 1) merge all of them into one!
      final UnboundedSource.CheckpointMark mergedCheckpoint = createMergedCheckpointMarks(kafkaOffloadingOutputs);
      final SourceVertexDataFetcher oSourceVertexDataFetcher = sourceVertexDataFetchers.get(0);
      final BeamUnboundedSourceVertex oSourceVertex = (BeamUnboundedSourceVertex) oSourceVertexDataFetcher.getDataSource();
      final UnboundedSource oSource = oSourceVertex.getUnboundedSource();

      final UnboundedSourceReadable newReadable =
        new UnboundedSourceReadable(oSource, null, mergedCheckpoint);

      oSourceVertexDataFetcher.setReadable(newReadable);
      availableFetchers.add(oSourceVertexDataFetcher);

      kafkaOffloadingOutputs.clear();
    }
  }

  @Override
  public void offloadingData(Object event, List<String> nextOperatorIds, long wm,
                             final String edgeId) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public synchronized void handleEndOffloadingEvent() {
    if (!checkSourceValidation()) {
      return;
    }
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

    } else if (taskStatus.compareAndSet(TaskExecutor.Status.OFFLOAD_PENDING, TaskExecutor.Status.RUNNING)) {
      taskExecutor.getPrevOffloadEndTime().set(System.currentTimeMillis());
      taskTimeMap.clear();
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


      if (!runningWorkers.isEmpty()) {
        throw new RuntimeException("Offload pending should not have running workers!: " + runningWorkers.size());
      }

    } else {
      throw new RuntimeException("Invalid task status " + taskStatus);
    }
  }

  private boolean checkSourceValidation(){
    if (sourceVertexDataFetchers.size() > 1) {
      return false;
    }

    if (!(sourceVertexDataFetchers.get(0) instanceof SourceVertexDataFetcher)) {
      return false;
    }

    return true;
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
    if (!checkSourceValidation()) {
      return;
    }

    final DAG<IRVertex, RuntimeEdge<IRVertex>> copyDag = SerializationUtils.deserialize(serializedDag);

    final OffloadingTask offloadingTask;
    if (sourceVertexDataFetcher != null) {
      final UnboundedSourceReadable readable = (UnboundedSourceReadable) sourceVertexDataFetcher.getReadable();
      final KafkaCheckpointMark checkpointMark = (KafkaCheckpointMark) readable.getReader().getCheckpointMark();
      final KafkaUnboundedSource unboundedSource = (KafkaUnboundedSource) readable.getUnboundedSource();
      final Coder<UnboundedSource.CheckpointMark> checkpointMarkCoder = unboundedSource.getCheckpointMarkCoder();

      offloadingTask = new OffloadingTask(
        executorId,
        taskId,
        RuntimeIdManager.getIndexFromTaskId(taskId),
        evalConf.samplingJson,
        copyDag,
        taskOutgoingEdges,
        task.getTaskOutgoingEdges(),
        task.getTaskIncomingEdges(),
        checkpointMark,
        unboundedSource);
    } else {
      offloadingTask = new OffloadingTask(
        executorId,
        taskId,
        RuntimeIdManager.getIndexFromTaskId(taskId),
        evalConf.samplingJson,
        copyDag,
        taskOutgoingEdges,
        task.getTaskOutgoingEdges(),
        task.getTaskIncomingEdges(),
        null,
        null);
    }

    // TODO: 1) Remove available and pending fetchers!!
    // Stop sources and output emission!!

    tinyWorkerManager.sendTask(offloadingTask, taskExecutor);

    prevOffloadStartTime.set(System.currentTimeMillis());

    if (!taskStatus.compareAndSet(TaskExecutor.Status.RUNNING, TaskExecutor.Status.OFFLOADED)) {
      LOG.warn("Multiple start request ... just ignore it");
      throw new RuntimeException("Invalid task status: " + taskStatus);
    }
  }

  @Override
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

  @Override
  public synchronized void handlePendingStreamingWorkers() {
    throw new RuntimeException("Cannot have pending worker!");

    if (kafkaOffloadPendingEvents.isEmpty()) {
      LOG.warn("HandlePendingStreamingWorker should be called with hasPendingStreamingWorker");
      return;
    }

    if (checkIsAllPendingReady()) {
      // 1. split source
      LOG.info("Ready to offloading !!: {}. status: {}", taskId, taskStatus);

      final SourceVertexDataFetcher dataFetcher = sourceVertexDataFetchers.get(0);
      final UnboundedSourceReadable readable = (UnboundedSourceReadable) dataFetcher.getReadable();
      final KafkaUnboundedSource unboundedSource = (KafkaUnboundedSource) readable.getUnboundedSource();

      final BeamUnboundedSourceVertex beamUnboundedSourceVertex = ((BeamUnboundedSourceVertex) dataFetcher.getDataSource());
      beamUnboundedSourceVertex.setUnboundedSource(unboundedSource);

      // 1. remove this data fetcher from current
      if (!availableFetchers.remove(dataFetcher)) {
        pendingFetchers.remove(dataFetcher);
      }

      // 2. get checkpoint mark
      final KafkaCheckpointMark unSplitCheckpointMark = (KafkaCheckpointMark) readable.getReader().getCheckpointMark();
      // 3. split sources and create new source vertex data fetcher
      final List<KafkaUnboundedSource> splitSources;
      try {
        splitSources = unboundedSource.split(1000, null);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      if (splitSources.size() != kafkaOffloadPendingEvents.size()) {
        throw new RuntimeException("Split num != pending event num: " + splitSources.size() + " , " + kafkaOffloadPendingEvents.size());
      }

      // 4. send to serverless
      LOG.info("Splitting source at {}, size: {}", taskId, splitSources.size());
      LOG.info("Execute streaming worker at {}!", taskId);

      // 5. Split checkpoint mark!!
      int index = 0;
      for (final KafkaOffloadingRequestEvent event : kafkaOffloadPendingEvents) {
        final KafkaUnboundedSource splitSource = splitSources.get(index);

        final BeamUnboundedSourceVertex sourceVertex =
          new BeamUnboundedSourceVertex(splitSource, null);

        final UnboundedSource.CheckpointMark splitCheckpointMark =
          createCheckpointMarkForSource(splitSource, unSplitCheckpointMark);

        final UnboundedSourceReadable newReadable =
          new UnboundedSourceReadable(splitSource, null, splitCheckpointMark);

        final SourceVertexDataFetcher sourceVertexDataFetcher =
          new SourceVertexDataFetcher(sourceVertex, dataFetcher.edge, newReadable, dataFetcher.getOutputCollector());

        final Coder<UnboundedSource.CheckpointMark> coder = splitSource.getCheckpointMarkCoder();
        final ControlMessage.Message message;
        try {
          LOG.info("Wait taskIndex... {}", taskId);
          message = event.taskIndexFuture.get();
          LOG.info("Receive taskIndex... {}/{}", taskId, message.getTaskIndexInfoMsg().getTaskIndex());
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }

        final ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.buffer();
        final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);
        try {
          bos.writeInt(event.id);
          bos.writeLong(message.getTaskIndexInfoMsg().getTaskIndex());
          coder.encode(splitCheckpointMark, bos);
          SerializationUtils.serialize(splitSource, bos);
          bos.close();
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }

        LOG.info("Offloading source id: {} for {} write: {} ... pending: {}", event.id, taskId, splitCheckpointMark);

        // put
        offloadedDataFetcherMap.put(event.id, new KafkaOffloadingDataEvent(
          event.offloadingWorker,
          splitSource,
          event.id,
          sourceVertexDataFetcher,
          splitCheckpointMark));

        event.offloadingWorker.execute(byteBuf, 0, false);
        runningWorkers.add(event.offloadingWorker);

        index += 1;
      }

      LOG.info("Finished offloading at {}", taskId);
      kafkaOffloadPendingEvents.clear();


      LOG.info("Before setting offloaded status: " + taskStatus);
      taskExecutor.getPrevOffloadStartTime().set(System.currentTimeMillis());
      if (!taskStatus.compareAndSet(TaskExecutor.Status.OFFLOAD_PENDING, TaskExecutor.Status.OFFLOADED)) {
        throw new RuntimeException("Invalid task status: " + taskStatus);
      } else {
        // we should emit end message
        irVertexDag.getTopologicalSort().stream().forEach(irVertex -> {
          if (irVertex instanceof OperatorVertex) {
            final Transform transform = ((OperatorVertex) irVertex).getTransform();
            if (transform instanceof GBKPartialTransform) {
              final OutputCollector outputCollector = taskExecutor.getVertexOutputCollector(irVertex.getId());
              final byte[] snapshot = SerializationUtils.serialize(transform);
              transform.flush();
              final Transform des = SerializationUtils.deserialize(snapshot);
              des.prepare(
                new TransformContextImpl(null, null, null), outputCollector);
              ((OperatorVertex)irVertex).setTransform(des);
            } else {
              transform.flush();
            }
          }
        });
        LOG.info("Close current output contexts");

        outputWriters.forEach(writer -> {
          LOG.info("Stopping writer {}", writer);
          writer.stop();
        });
      }
    }
  }
}
