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
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.compiler.frontend.beam.source.BeamUnboundedSourceVertex;
import org.apache.nemo.compiler.frontend.beam.source.UnboundedSourceReadable;
import org.apache.nemo.conf.EvalConf;
import org.apache.nemo.offloading.client.LambdaOffloadingWorkerFactory;
import org.apache.nemo.offloading.client.StreamingWorkerService;
import org.apache.nemo.offloading.common.OffloadingWorker;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransport;
import org.apache.nemo.runtime.executor.common.DataFetcher;
import org.apache.nemo.runtime.executor.common.SourceVertexDataFetcher;
import org.apache.nemo.runtime.executor.data.SerializerManager;
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

public final class KafkaOffloader {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaOffloader.class.getName());

  private final StreamingWorkerService streamingWorkerService;

  private final byte[] serializedDag;
  private final LambdaOffloadingWorkerFactory lambdaOffloadingWorkerFactory;
  private final Map<String, List<String>> taskOutgoingEdges;
  private final SerializerManager serializerManager;
  private final ConcurrentLinkedQueue<Object> offloadingEventQueue;
  private final List<SourceVertexDataFetcher> sourceVertexDataFetchers;
  private final String taskId;
  private final List<DataFetcher> availableFetchers;
  private final List<DataFetcher> pendingFetchers;
  final ExecutorService closeService = Executors.newSingleThreadExecutor();


  private static final AtomicInteger sourceId = new AtomicInteger(0);

  private final List<OffloadingWorker> runningWorkers = new ArrayList<>();
  final ConcurrentMap<Integer, KafkaOffloadingDataEvent> offloadedDataFetcherMap = new ConcurrentHashMap<>();
  final List<Pair<SourceVertexDataFetcher, UnboundedSource.CheckpointMark>> reExecutedDataFetchers = new ArrayList<>();
  final Queue<KafkaOffloadingDataEvent> kafkaOffloadPendingEvents = new LinkedBlockingQueue<>();

  private final AtomicReference<TaskExecutor.Status> taskStatus;

  private final ScheduledExecutorService logger = Executors.newSingleThreadScheduledExecutor();

  private final AtomicLong prevOffloadStartTime;
  private final AtomicLong prevOffloadEndTime;

  private final Map<String, InetSocketAddress> executorAddressMap;
  private final Map<Integer, String> dstTaskIndexTargetExecutorMap;
  private final String executorId;
  private final Task task;

  private final AtomicInteger offloadedIndex;
  private final EvalConf evalConf;

  public KafkaOffloader(final String executorId,
                        final Task task,
                        final EvalConf evalConf,
                        final Map<String, InetSocketAddress> executorAddressMap,
                        final Map<Integer, String> dstTaskIndexTargetExecutorMap,
                        final byte[] serializedDag,
                        final LambdaOffloadingWorkerFactory lambdaOffloadingWorkerFactory,
                        final Map<String, List<String>> taskOutgoingEdges,
                        final SerializerManager serializerManager,
                        final ConcurrentLinkedQueue<Object> offloadingEventQueue,
                        final List<SourceVertexDataFetcher> sourceVertexDataFetchers,
                        final String taskId,
                        final List<DataFetcher> availableFetchers,
                        final List<DataFetcher> pendingFetchers,
                        final AtomicReference<TaskExecutor.Status> taskStatus,
                        final AtomicLong prevOffloadStartTime,
                        final AtomicLong prevOffloadEndTime) {
    this.executorId = executorId;
    this.offloadedIndex = new AtomicInteger(RuntimeIdManager.getIndexFromTaskId(task.getTaskId()));
    offloadedIndex.getAndIncrement();
    this.task = task;
    this.evalConf = evalConf;
    this.executorAddressMap = executorAddressMap;
    this.dstTaskIndexTargetExecutorMap = dstTaskIndexTargetExecutorMap;
    this.serializedDag = serializedDag;
    this.lambdaOffloadingWorkerFactory = lambdaOffloadingWorkerFactory;
    this.taskOutgoingEdges = taskOutgoingEdges;
    this.serializerManager = serializerManager;
    this.offloadingEventQueue = offloadingEventQueue;
    this.sourceVertexDataFetchers = sourceVertexDataFetchers;
    this.taskId = taskId;
    this.availableFetchers = availableFetchers;
    this.pendingFetchers = pendingFetchers;
    this.streamingWorkerService = createStreamingWorkerService();
    this.taskStatus = taskStatus;
    this.prevOffloadEndTime = prevOffloadEndTime;
    this.prevOffloadStartTime = prevOffloadStartTime;

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

  private StreamingWorkerService createStreamingWorkerService() {
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

    final SourceVertexDataFetcher dataFetcher = sourceVertexDataFetchers.get(0);
    final UnboundedSourceReadable readable = (UnboundedSourceReadable) dataFetcher.getReadable();
    final UnboundedSource unboundedSource = readable.getUnboundedSource();

    final StreamingWorkerService streamingWorkerService =
      new StreamingWorkerService(lambdaOffloadingWorkerFactory,
        new KafkaOffloadingTransform(
          executorId,
          offloadedIndex.getAndIncrement(),
          evalConf.samplingJson,
          copyDag,
          taskOutgoingEdges,
          executorAddressMap,
          serializerManager.runtimeEdgeIdToSerializer,
          dstTaskIndexTargetExecutorMap,
          task.getTaskOutgoingEdges()),
        new KafkaOffloadingSerializer(serializerManager.runtimeEdgeIdToSerializer,
          unboundedSource.getCheckpointMarkCoder()),
        new StatelessOffloadingEventHandler(offloadingEventQueue));

    return streamingWorkerService;
  }

  private KafkaCheckpointMark createMergedCheckpointMarks(
    final List<Pair<SourceVertexDataFetcher, UnboundedSource.CheckpointMark>> list) {
    final List<KafkaCheckpointMark.PartitionMark> partitionMarks =
      list.stream()
        .map(pair -> {
          final SourceVertexDataFetcher sourceVertexDataFetcher = pair.left();
          // SEND checkpoint and unbounded source
          final UnboundedSourceReadable readable = (UnboundedSourceReadable) sourceVertexDataFetcher.getReadable();
          final KafkaCheckpointMark cmark;

          if (sourceVertexDataFetcher.isStarted()) {
            try {
              cmark = (KafkaCheckpointMark) readable.getReader().getCheckpointMark();
            } catch (NullPointerException e) {
              e.printStackTrace();
              throw new RuntimeException("Null at creating merged checkpoint source " + readable + ", " + sourceVertexDataFetcher);
            }
          } else {
            cmark = (KafkaCheckpointMark) pair.right();
          }

          return cmark.getPartitions().get(0);
        })
        .collect(Collectors.toList());

    partitionMarks.sort((o1, o2) -> {
        return o1.getPartition() - o2.getPartition();
    });

    return new KafkaCheckpointMark(partitionMarks, Optional.empty());
  }

  public synchronized void handleKafkaOffloadingOutput(final KafkaOffloadingOutput output) {
    // handling checkpoint mark to resume the kafka source reading
    // Serverless -> VM
    // we start to read kafka events again
    final int id = output.id;
    final KafkaCheckpointMark checkpointMark = (KafkaCheckpointMark) output.checkpointMark;
    LOG.info("Receive checkpoint mark for source {} in VM: {}", id, checkpointMark);

    if (!offloadedDataFetcherMap.containsKey(id)) {
      throw new RuntimeException("Source " + id + " is not offloaded yet!");
    }

    final SourceVertexDataFetcher offloadedDataFetcher = offloadedDataFetcherMap.remove(id).sourceVertexDataFetcher;

    restartDataFetcher(offloadedDataFetcher, checkpointMark, id);

    if (offloadedDataFetcherMap.isEmpty()) {
      // It means that offloading finished
      if (!taskStatus.compareAndSet(TaskExecutor.Status.DEOFFLOAD_PENDING, TaskExecutor.Status.RUNNING)) {
        throw new RuntimeException("Invalid task status: " + taskStatus);
      }

      LOG.info("Merge {} sources into one", reExecutedDataFetchers.size());
      // TODO: merge sources!!
      // 1) merge all of them into one!
      final UnboundedSource.CheckpointMark mergedCheckpoint = createMergedCheckpointMarks(reExecutedDataFetchers);
      final SourceVertexDataFetcher oSourceVertexDataFetcher = sourceVertexDataFetchers.get(0);
      final BeamUnboundedSourceVertex oSourceVertex = (BeamUnboundedSourceVertex) oSourceVertexDataFetcher.getDataSource();
      final UnboundedSource oSource = oSourceVertex.getUnboundedSource();

      final UnboundedSourceReadable newReadable =
        new UnboundedSourceReadable(oSource, null, mergedCheckpoint);

      oSourceVertexDataFetcher.setReadable(newReadable);
      availableFetchers.add(oSourceVertexDataFetcher);

      // 2) remove reExecute data fetchers
      // close all data fetchers
      reExecutedDataFetchers.stream().forEach(pair -> {
        if (!availableFetchers.remove(pair.left())) {
          pendingFetchers.remove(pair.left());
        }
      });

      final List<SourceVertexDataFetcher> closeExecutors =
        reExecutedDataFetchers.stream()
        .map(Pair::left)
        .collect(Collectors.toList());

      closeService.execute(() -> {
        closeExecutors.stream().forEach(dataFetcher -> {
          if (dataFetcher.isStarted()) {
            try {
              dataFetcher.getReadable().close();
            } catch (IOException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
          }
        });
      });

      reExecutedDataFetchers.clear();
    }
  }

  private void restartDataFetcher(final SourceVertexDataFetcher offloadedDataFetcher,
                                  final UnboundedSource.CheckpointMark checkpointMark,
                                  final int id) {
    final BeamUnboundedSourceVertex beamUnboundedSourceVertex = (BeamUnboundedSourceVertex) offloadedDataFetcher.getDataSource();
    final UnboundedSource unboundedSource = beamUnboundedSourceVertex.getUnboundedSource();

    final UnboundedSourceReadable readable =
      new UnboundedSourceReadable(unboundedSource, null, checkpointMark);
    offloadedDataFetcher.setReadable(readable);

    LOG.info("Restart source {} at checkpointmark {}", id, checkpointMark);
    availableFetchers.add(offloadedDataFetcher);
    reExecutedDataFetchers.add(Pair.of(offloadedDataFetcher, checkpointMark));
  }

  public synchronized void handleEndOffloadingKafkaEvent() {
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

    } else if (taskStatus.compareAndSet(TaskExecutor.Status.OFFLOAD_PENDING, TaskExecutor.Status.DEOFFLOAD_PENDING)) {
      // It means that this is not initialized yet
      // just finish this worker!
      for (final KafkaOffloadingDataEvent event : kafkaOffloadPendingEvents) {
        event.offloadingWorker.forceClose();
        // restart the workers
        // * This is already running... we don't have to restart it
        //LOG.info("Just restart source {} init workers at {}", event.id, taskId);
        //restartDataFetcher(event.sourceVertexDataFetcher, event.checkpointMark, event.id);
      }

      kafkaOffloadPendingEvents.clear();

      if (runningWorkers.isEmpty()) {
        taskStatus.compareAndSet(TaskExecutor.Status.DEOFFLOAD_PENDING, TaskExecutor.Status.RUNNING);
      } else {
        // We will wait for the checkpoint mark of these workers
        // and restart the workers
        for (final OffloadingWorker runningWorker : runningWorkers) {
          LOG.info("Closing running worker {} at {}", runningWorker.getId(), taskId);
          runningWorker.forceClose();
        }
        runningWorkers.clear();
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

  public synchronized void handleStartOffloadingKafkaEvent() {
    if (!checkSourceValidation()) {
      return;
    }

    prevOffloadStartTime.set(System.currentTimeMillis());

    if (!taskStatus.compareAndSet(TaskExecutor.Status.RUNNING, TaskExecutor.Status.OFFLOAD_PENDING)) {
        throw new RuntimeException("Invalid task status: " + taskStatus);
    }

    // KAFKA SOURCE OFFLOADING !!!!
    // VM -> Serverless
    if (sourceVertexDataFetchers.size() > 1) {
      throw new RuntimeException("Unsupport > 1 size sources");
    }

    if (!kafkaOffloadPendingEvents.isEmpty()) {
      LOG.warn("Task {} received start offloading, but it still offloads sources {}",
        taskId, kafkaOffloadPendingEvents.size());
      // still offloading data fetchers.. skip
      return;
    }

    if (!reExecutedDataFetchers.isEmpty()) {
      // not merged yet... just offload them
      LOG.info("Task {} Offload re-executed data fetchers: {}", taskId, reExecutedDataFetchers.size());

      for (final Pair<SourceVertexDataFetcher, UnboundedSource.CheckpointMark> pair : reExecutedDataFetchers) {
        final SourceVertexDataFetcher reExecutedFetcher = pair.left();
        final UnboundedSource.CheckpointMark cMark = pair.right();

        final BeamUnboundedSourceVertex beamUnboundedSourceVertex =
          (BeamUnboundedSourceVertex) reExecutedFetcher.getDataSource();
        final UnboundedSource unboundedSource = beamUnboundedSourceVertex.getUnboundedSource();
        final OffloadingWorker worker = streamingWorkerService.createStreamWorker();
        final UnboundedSourceReadable readable = (UnboundedSourceReadable) reExecutedFetcher.getReadable();
        final UnboundedSource.CheckpointMark checkpointMark = cMark;

        // 1. remove this data fetcher from current
        /* do not remove
        if (!availableFetchers.remove(reExecutedFetcher)) {
          pendingFetchers.remove(reExecutedFetcher);
        }
        */

        kafkaOffloadPendingEvents.add(new KafkaOffloadingDataEvent(
          worker, unboundedSource, sourceId.getAndIncrement(), reExecutedFetcher, checkpointMark));

        if (reExecutedFetcher.isStarted()) {
          closeService.execute(() -> {
            try {
              reExecutedFetcher.getReadable().close();
            } catch (IOException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
          });
        }
      }

      reExecutedDataFetchers.clear();
    } else {

      final SourceVertexDataFetcher dataFetcher = sourceVertexDataFetchers.get(0);
      final UnboundedSourceReadable readable = (UnboundedSourceReadable) dataFetcher.getReadable();
      final UnboundedSource unboundedSource = readable.getUnboundedSource();
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

      // 4. send to serverless
      LOG.info("Splitting source at {}, size: {}", taskId, splitSources.size());
      LOG.info("Execute streaming worker at {}!", taskId);

      // 5. Split checkpoint mark!!
      splitSources.parallelStream().forEach(splitSource -> {

        final BeamUnboundedSourceVertex sourceVertex =
          new BeamUnboundedSourceVertex(splitSource, null);

        final UnboundedSource.CheckpointMark splitCheckpointMark =
          createCheckpointMarkForSource(splitSource, unSplitCheckpointMark);

        final UnboundedSourceReadable newReadable =
          new UnboundedSourceReadable(splitSource, null, splitCheckpointMark);

        final SourceVertexDataFetcher sourceVertexDataFetcher =
          new SourceVertexDataFetcher(sourceVertex, dataFetcher.edge, newReadable, dataFetcher.getOutputCollector());

        final OffloadingWorker worker = streamingWorkerService.createStreamWorker();

        kafkaOffloadPendingEvents.add(new KafkaOffloadingDataEvent(
          worker, splitSource, sourceId.getAndIncrement(), sourceVertexDataFetcher, splitCheckpointMark));
      });

      // 6. add it to available fetchers!
      availableFetchers.addAll(kafkaOffloadPendingEvents.stream()
        .map(kafkaOffloadingDataEvent ->
          kafkaOffloadingDataEvent.sourceVertexDataFetcher).collect(Collectors.toList()));

      // 7. close current fetcher and start split data fetchers
      if (dataFetcher.isStarted()) {
        try {
          readable.close();
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    }
  }

  public boolean hasPendingStraemingWorkers() {
    return !kafkaOffloadPendingEvents.isEmpty();
  }

  public synchronized void handlePendingStreamingWorkers() {

    if (kafkaOffloadPendingEvents.isEmpty()) {
      throw new RuntimeException("HandlePendingStreamingWorker should be called with hasPendingStreamingWorker");
    }

    // Find whether there are ready workers
    //final Iterator<Pair<OffloadingWorker, UnboundedSource>> iterator = initStreamingWorkers.iterator();
    final Iterator<KafkaOffloadingDataEvent> iterator = kafkaOffloadPendingEvents.iterator();
    while (iterator.hasNext()) {
      final KafkaOffloadingDataEvent event = iterator.next();
      final OffloadingWorker initWorker = event.offloadingWorker;
      final UnboundedSource unboundedSource = event.unboundedSource;

      if (initWorker.isReady()) {
        iterator.remove();

        // stop one of the split data fetchers!
        final Integer id = event.id;
        final SourceVertexDataFetcher splitDataFetcher = event.sourceVertexDataFetcher;

        // remove from available and pending
        if (!availableFetchers.remove(splitDataFetcher)) {
          pendingFetchers.remove(splitDataFetcher);
        }

        // SEND checkpoint and unbounded source
        final UnboundedSourceReadable readable = (UnboundedSourceReadable) splitDataFetcher.getReadable();
        final UnboundedSource.CheckpointMark checkpointMark;

        if (splitDataFetcher.isStarted()) {
          try {
            checkpointMark = readable.getReader().getCheckpointMark();
          } catch (NullPointerException e) {
            e.printStackTrace();
            throw new RuntimeException("Null at handling source " + id + ", readable: " + readable);
          }
        } else {
          checkpointMark = event.checkpointMark;
        }

        final Coder<UnboundedSource.CheckpointMark> coder = unboundedSource.getCheckpointMarkCoder();

        final ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.buffer();
        final ByteBufOutputStream bos = new ByteBufOutputStream(byteBuf);
        try {
          bos.writeInt(id);
          coder.encode(checkpointMark, bos);
          SerializationUtils.serialize(unboundedSource, bos);
          bos.close();
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }

        LOG.info("Offloading source id: {} for {} write: {} ... pending: {}", id, taskId, checkpointMark,
          kafkaOffloadPendingEvents.size());

        // put
        offloadedDataFetcherMap.put(id, event);

        initWorker.execute(byteBuf, 0, false);
        runningWorkers.add(initWorker);

        if (splitDataFetcher.isStarted()) {
          closeService.execute(() -> {
            try {
              readable.close();
            } catch (IOException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
          });
        }
      }
    }

    // Change status to offloaded
    if (kafkaOffloadPendingEvents.isEmpty()) {
      if (!taskStatus.compareAndSet(TaskExecutor.Status.OFFLOAD_PENDING, TaskExecutor.Status.OFFLOADED)) {
        throw new RuntimeException("Invalid task status: " + taskStatus);
      }
    }
  }


}
