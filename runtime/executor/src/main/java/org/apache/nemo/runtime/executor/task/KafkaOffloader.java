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
import org.apache.nemo.offloading.client.LambdaOffloadingWorkerFactory;
import org.apache.nemo.offloading.client.StreamingWorkerService;
import org.apache.nemo.offloading.common.OffloadingWorker;
import org.apache.nemo.runtime.executor.common.DataFetcher;
import org.apache.nemo.runtime.executor.common.SourceVertexDataFetcher;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingOutput;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingSerializer;
import org.apache.nemo.runtime.lambdaexecutor.kafka.KafkaOffloadingTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
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
  final ExecutorService closeService = Executors.newCachedThreadPool();


  private static final AtomicInteger sourceId = new AtomicInteger(0);

  private final List<OffloadingWorker> runningWorkers = new ArrayList<>();
  final Map<Integer, SourceVertexDataFetcher> offloadedDataFetcherMap = new ConcurrentHashMap<>();
  final List<Pair<SourceVertexDataFetcher, UnboundedSource.CheckpointMark>> reExecutedDataFetchers = new ArrayList<>();
  final Queue<KafkaOffloadingDataEvent> kafkaOffloadPendingEvents = new LinkedBlockingQueue<>();

  public KafkaOffloader(final byte[] serializedDag,
                        final LambdaOffloadingWorkerFactory lambdaOffloadingWorkerFactory,
                        final Map<String, List<String>> taskOutgoingEdges,
                        final SerializerManager serializerManager,
                        final ConcurrentLinkedQueue<Object> offloadingEventQueue,
                        final List<SourceVertexDataFetcher> sourceVertexDataFetchers,
                        final String taskId,
                        final List<DataFetcher> availableFetchers,
                        final List<DataFetcher> pendingFetchers) {
    this.serializedDag = serializedDag;
    this.lambdaOffloadingWorkerFactory = lambdaOffloadingWorkerFactory;
    this.taskOutgoingEdges = taskOutgoingEdges;
    this.serializerManager = serializerManager;
    this.offloadingEventQueue = offloadingEventQueue;
    this.sourceVertexDataFetchers = sourceVertexDataFetchers;
    this. taskId = taskId;
    this.availableFetchers = availableFetchers;
    this.pendingFetchers = pendingFetchers;
    this.streamingWorkerService = createStreamingWorkerService();
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
        new KafkaOffloadingTransform(copyDag, taskOutgoingEdges),
        new KafkaOffloadingSerializer(serializerManager.runtimeEdgeIdToSerializer,
          unboundedSource.getCheckpointMarkCoder()),
        new StatelessOffloadingEventHandler(offloadingEventQueue));

    return streamingWorkerService;
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

    final SourceVertexDataFetcher offloadedDataFetcher = offloadedDataFetcherMap.remove(id);

    restartDataFetcher(offloadedDataFetcher, checkpointMark, id);

    if (offloadedDataFetcherMap.isEmpty()) {
      // TODO: merge sources!!
      // 1) remove reExecute data fetchers
      // 2) merge all of them into one!
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
    if (!kafkaOffloadPendingEvents.isEmpty()) {
      // It means that this is not initialized yet
      // just finish this worker!
      for (final KafkaOffloadingDataEvent event : kafkaOffloadPendingEvents) {
        event.offloadingWorker.forceClose();
        // restart the workers
        LOG.info("Just restart source {} init workers at {}", event.id, taskId);
        restartDataFetcher(event.sourceVertexDataFetcher, event.checkpointMark, event.id);
      }

      kafkaOffloadPendingEvents.clear();
    }

    for (final OffloadingWorker runningWorker : runningWorkers) {
      LOG.info("Closing running worker {} at {}", runningWorker.getId(), taskId);
      runningWorker.forceClose();
    }

    runningWorkers.clear();
  }

  public synchronized void handleStartOffloadingKafkaEvent() {

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
        final UnboundedSource.CheckpointMark checkpointMark;
        if (reExecutedFetcher.isStarted()) {
          checkpointMark = readable.getReader().getCheckpointMark();
        } else {
          checkpointMark =  cMark;
        }

        // 1. remove this data fetcher from current
        if (!availableFetchers.remove(reExecutedFetcher)) {
          pendingFetchers.remove(reExecutedFetcher);
        }

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
      /*
      availableFetchers.addAll(kafkaOffloadPendingEvents.stream()
        .map(kafkaOffloadingDataEvent ->
          kafkaOffloadingDataEvent.sourceVertexDataFetcher).collect(Collectors.toList()));
          */

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

    // Find whether there are ready workers
    if (!kafkaOffloadPendingEvents.isEmpty()) {
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
          // don't have to remove because we do not add it
          /*
          if (!availableFetchers.remove(splitDataFetcher)) {
            pendingFetchers.remove(splitDataFetcher);
          }
          */

          // SEND checkpoint and unbounded source
          final UnboundedSourceReadable readable = (UnboundedSourceReadable) splitDataFetcher.getReadable();
          final UnboundedSource.CheckpointMark checkpointMark = event.checkpointMark;
          /*
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
          */

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
          offloadedDataFetcherMap.put(id, splitDataFetcher);

          initWorker.execute(byteBuf, 0, false);
          runningWorkers.add(initWorker);

          /* already closed
          if (splitDataFetcher.isStarted()) {
            try {
              readable.close();
            } catch (IOException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
          }
          */
        }
      }
    }

  }


}
