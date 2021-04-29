package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.common.coder.BytesDecoderFactory;
import org.apache.nemo.common.coder.BytesEncoderFactory;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.exception.UnsupportedCommPatternException;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.Task;
import org.apache.nemo.common.ir.vertex.utility.ConditionalRouterVertex;
import org.apache.nemo.offloading.common.StateStore;
import org.apache.nemo.runtime.executor.common.controlmessages.TaskControlMessage;
import org.apache.nemo.runtime.executor.common.datatransfer.*;
import org.apache.nemo.runtime.executor.common.tasks.TaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public final class TaskExecutorUtil {
  private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorUtil.class.getName());


  public static int taskIncomingEdgeDoneAckCounter(final Task task) {
    final AtomicInteger cnt = new AtomicInteger(0);

    task.getTaskIncomingEdges()
      .forEach(inEdge -> {
        if (inEdge.getDataCommunicationPattern()
          .equals(CommunicationPatternProperty.Value.TransientOneToOne)
          || inEdge.getDataCommunicationPattern()
          .equals(CommunicationPatternProperty.Value.OneToOne)) {
          cnt.getAndIncrement();
        } else {
          final int parallelism = inEdge.getSrcIRVertex()
            .getPropertyValue(ParallelismProperty.class).get();
          cnt.getAndAdd(parallelism);
        }
      });

    return cnt.get();
  }

  public static int taskOutgoingEdgeDoneAckCounter(final Task task) {
    final AtomicInteger cnt = new AtomicInteger(0);

    task.getTaskOutgoingEdges()
      .forEach(outgoingEdge -> {
        if (outgoingEdge.getDataCommunicationPattern()
          .equals(CommunicationPatternProperty.Value.TransientOneToOne)
          || outgoingEdge.getDataCommunicationPattern()
          .equals(CommunicationPatternProperty.Value.OneToOne)) {
          cnt.getAndIncrement();
        } else {
          final int parallelism = outgoingEdge.getSrcIRVertex()
            .getPropertyValue(ParallelismProperty.class).get();
          cnt.getAndAdd(parallelism);
        }
      });

    return cnt.get();
  }

  public static void sendOutputDoneMessage(final Task task,
                                           final PipeManagerWorker pipeManagerWorker,
                                           final TaskControlMessage.TaskControlMessageType type) {

    final String srcTask = task.getTaskId();
    final int index = RuntimeIdManager.getIndexFromTaskId(task.getTaskId());

    task.getTaskOutgoingEdges()
      .forEach(outgoingEdge -> {
        if (outgoingEdge.getDataCommunicationPattern()
          .equals(CommunicationPatternProperty.Value.TransientOneToOne)
          || outgoingEdge.getDataCommunicationPattern()
          .equals(CommunicationPatternProperty.Value.OneToOne)) {

          final String dstTaskId =
            RuntimeIdManager.generateTaskId(outgoingEdge.getDst().getId(), index, 0);
          pipeManagerWorker.writeControlMessage(srcTask, outgoingEdge.getId(), dstTaskId,
            type,
            null);

          LOG.info("Send task output done signal from {} to {}", srcTask,
            dstTaskId);

        } else {
          final int parallelism = outgoingEdge.getSrcIRVertex()
            .getPropertyValue(ParallelismProperty.class).get();

          for (int i = 0; i < parallelism; i++) {
            final String dstTaskId =
              RuntimeIdManager.generateTaskId(outgoingEdge.getDst().getId(), i, 0);
            pipeManagerWorker.writeControlMessage(srcTask, outgoingEdge.getId(), dstTaskId,
              type,
              null);

            LOG.info("Send task output done signal from {} to {}", srcTask,
              dstTaskId);
          }
        }
      });

  }


  public static List<String> getSrcTaskIds(final String taskId,
                                           final RuntimeEdge runtimeEdge) {
    final Optional<CommunicationPatternProperty.Value> comValue =
      runtimeEdge.getPropertyValue(CommunicationPatternProperty.class);

    final StageEdge stageEdge = (StageEdge) runtimeEdge;
    final int index = RuntimeIdManager.getIndexFromTaskId(taskId);

    final List<String> dstTaskIds;
    if (comValue.get().equals(CommunicationPatternProperty.Value.OneToOne)
      || comValue.get().equals(CommunicationPatternProperty.Value.TransientOneToOne)) {
      dstTaskIds = Collections.singletonList(
        RuntimeIdManager.generateTaskId(stageEdge.getSrc().getId(), index, 0));
    } else if (comValue.get().equals(CommunicationPatternProperty.Value.BroadCast)
      || comValue.get().equals(CommunicationPatternProperty.Value.Shuffle)
      || comValue.get().equals(CommunicationPatternProperty.Value.TransientShuffle)
      || comValue.get().equals(CommunicationPatternProperty.Value.TransientRR)
      || comValue.get().equals(CommunicationPatternProperty.Value.RoundRobin) ) {

      final List<Integer> dstIndices = stageEdge.getSrc().getTaskIndices();
      dstTaskIds =
        dstIndices.stream()
          .map(dstTaskIndex ->
            RuntimeIdManager.generateTaskId(stageEdge.getSrc().getId(), dstTaskIndex, 0))
          .collect(Collectors.toList());
      LOG.info("Writing data: edge: {}, Task {}, Dest {}", runtimeEdge.getId(), taskId, dstIndices);
    } else {
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
    return dstTaskIds;
  }


  public static List<String> getDstTaskIds(final String taskId,
                                     final RuntimeEdge runtimeEdge) {
    final Optional<CommunicationPatternProperty.Value> comValue =
      runtimeEdge.getPropertyValue(CommunicationPatternProperty.class);

    final StageEdge stageEdge = (StageEdge) runtimeEdge;
    final int index = RuntimeIdManager.getIndexFromTaskId(taskId);

    final List<String> dstTaskIds;
    if (comValue.get().equals(CommunicationPatternProperty.Value.OneToOne)
      || comValue.get().equals(CommunicationPatternProperty.Value.TransientOneToOne)) {
      dstTaskIds = Collections.singletonList(
        RuntimeIdManager.generateTaskId(stageEdge.getDst().getId(), index, 0));
    } else if (comValue.get().equals(CommunicationPatternProperty.Value.BroadCast)
      || comValue.get().equals(CommunicationPatternProperty.Value.Shuffle)
      || comValue.get().equals(CommunicationPatternProperty.Value.TransientShuffle)
      || comValue.get().equals(CommunicationPatternProperty.Value.TransientRR)
      || comValue.get().equals(CommunicationPatternProperty.Value.RoundRobin) ) {

      final List<Integer> dstIndices = stageEdge.getDst().getTaskIndices();
      dstTaskIds =
        dstIndices.stream()
          .map(dstTaskIndex ->
            RuntimeIdManager.generateTaskId(stageEdge.getDst().getId(), dstTaskIndex, 0))
          .collect(Collectors.toList());
      LOG.info("Writing data: edge: {}, Task {}, Dest {}", runtimeEdge.getId(), taskId, dstIndices);
    } else {
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
    return dstTaskIds;
  }

  public static void sendInitMessage(final Task task,
                                     final InputPipeRegister inputPipeRegister) {
    task.getTaskOutgoingEdges().forEach(edge -> {
      final Integer taskIndex = RuntimeIdManager.getIndexFromTaskId(task.getTaskId());

      // bidrectional !!
      final int parallelism = edge
        .getDstIRVertex().getPropertyValue(ParallelismProperty.class).get();

      final CommunicationPatternProperty.Value comm =
        edge.getPropertyValue(CommunicationPatternProperty.class).get();

      if (comm.equals(CommunicationPatternProperty.Value.OneToOne)
        || comm.equals(CommunicationPatternProperty.Value.TransientOneToOne)) {
        inputPipeRegister.sendPipeInitMessage(
          RuntimeIdManager.generateTaskId(edge.getDst().getId(), taskIndex, 0),
          edge.getId(),
          task.getTaskId());
      } else {
        for (int i = 0; i < parallelism; i++) {
          inputPipeRegister.sendPipeInitMessage(
            RuntimeIdManager.generateTaskId(edge.getDst().getId(), i, 0),
            edge.getId(),
            task.getTaskId());
        }
      }
    });

    final int taskIndex = RuntimeIdManager.getIndexFromTaskId(task.getTaskId());

    // Traverse in a reverse-topological order to ensure that each visited vertex's children vertices exist.
    final List<IRVertex> reverseTopologicallySorted = new ArrayList<>(task.getIrDag().getTopologicalSort());
    Collections.reverse(reverseTopologicallySorted);

    task.getTaskIncomingEdges()
      .forEach(edge -> {
        // LOG.info("Adding data fetcher for {} / {}", taskId, irVertex.getId());
        final int parallelism = edge
          .getSrcIRVertex().getPropertyValue(ParallelismProperty.class).get();

        final CommunicationPatternProperty.Value comm =
          edge.getPropertyValue(CommunicationPatternProperty.class).get();

        if (comm.equals(CommunicationPatternProperty.Value.OneToOne)
          || comm.equals(CommunicationPatternProperty.Value.TransientOneToOne)) {
          inputPipeRegister.sendPipeInitMessage(
            RuntimeIdManager.generateTaskId(edge.getSrc().getId(), taskIndex, 0),
            edge.getId(),
            task.getTaskId());
        } else {
          for (int i = 0; i < parallelism; i++) {
            inputPipeRegister.sendPipeInitMessage(
              RuntimeIdManager.generateTaskId(edge.getSrc().getId(), i, 0),
              edge.getId(),
              task.getTaskId());
          }
        }
      });
  }

  /*
  // TODO: set stateless
  public static void prepare(
    final Task task,
    final TaskExecutor taskExecutor,
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
    final IntermediateDataIOFactory intermediateDataIOFactory,
    final List<Transform> statefulTransforms,
    final Map<String, List<OutputWriter>> externalAdditionalOutputMap,
    final TaskMetrics taskMetrics,
    final OutputCollectorGenerator outputCollectorGenerator,
    final SerializerManager serializerManager,
    final Map<String, Double> samplingMap,
    final Map<String, Pair<OperatorMetricCollector, OutputCollector>> vertexIdAndCollectorMap,
    final Map<String, NextIntraTaskOperatorInfo> operatorInfoMap,
    final StateStore stateStore,
    final String executorId) {
    final int taskIndex = RuntimeIdManager.getIndexFromTaskId(task.getTaskId());

    // Traverse in a reverse-topological order to ensure that each visited vertex's children vertices exist.
    final List<IRVertex> reverseTopologicallySorted = new ArrayList<>(irVertexDag.getTopologicalSort());
    Collections.reverse(reverseTopologicallySorted);

    final String taskId = task.getTaskId();

    // Build a map for edge as a key and edge index as a value
    // This variable is used for creating NextIntraTaskOperatorInfo
    // in {@link this#getInternalMainOutputs and this#internalMainOutputs}
    reverseTopologicallySorted.forEach(childVertex -> {

      if (childVertex.isGBK) {
        if (childVertex instanceof OperatorVertex) {
          final OperatorVertex ov = (OperatorVertex) childVertex;
          statefulTransforms.add(ov.getTransform());
          LOG.info("Set GBK final transform");
        }
      }

      if (irVertexDag.getOutgoingEdgesOf(childVertex.getId()).size() == 0) {
        childVertex.isSink = true;

        LOG.info("Sink vertex: {}", childVertex.getId());
      }
    });

    // serializedDag = SerializationUtils.serialize(irVertexDag);

    final Map<String, OutputCollector> outputCollectorMap = new HashMap<>();

    // Create a harness for each vertex
    reverseTopologicallySorted.forEach(irVertex -> {
        final Optional<Readable> sourceReader = TaskExecutorUtil
          .getSourceVertexReader(irVertex, task.getIrVertexIdToReadable());

        if (sourceReader.isPresent() != irVertex instanceof SourceVertex) {
          throw new IllegalStateException(irVertex.toString());
        }

      externalAdditionalOutputMap.putAll(
        TaskExecutorUtil.getExternalAdditionalOutputMap(
          irVertex, task.getTaskOutgoingEdges(), intermediateDataIOFactory, taskId,
          taskMetrics));

        final OutputCollector outputCollector = outputCollectorGenerator
          .generate(irVertex,
            taskId,
            irVertexDag,
            taskExecutor,
            serializerManager,
            samplingMap,
            vertexIdAndCollectorMap,
            taskMetrics,
            task.getTaskOutgoingEdges(),
            operatorInfoMap,
            externalAdditionalOutputMap);

        outputCollectorMap.put(irVertex.getId(), outputCollector);

        // Create VERTEX HARNESS
        final Transform.Context context = new TransformContextImpl(
          irVertex, null, taskId, stateStore,
          null,
          executorId);

        TaskExecutorUtil.prepareTransform(irVertex, context, outputCollector, taskId);

        // Prepare data READ
        // Source read
        // TODO[SLS]: should consider multiple outgoing edges
        // All edges will have the same encoder/decoder!
        if (irVertex instanceof SourceVertex) {
          // final RuntimeEdge edge = irVertexDag.getOutgoingEdgesOf(irVertex).get(0);
          final RuntimeEdge edge = task.getTaskOutgoingEdges().get(0);
          // srcSerializer = serializerManager.getSerializer(edge.getId());
          // LOG.info("SourceVertex: {}, edge: {}, serializer: {}", irVertex.getId(), edge.getId(),
          // srcSerializer);

          // Source vertex read
          final SourceVertexDataFetcher fe = new SourceVertexDataFetcher(
            (SourceVertex) irVertex,
            edge,
            sourceReader.get(),
            outputCollector,
            prepareService,
            taskId,
            prepared,
            new Readable.ReadableContext() {
              @Override
              public StateStore getStateStore() {
                return stateStore;
              }

              @Override
              public String getTaskId() {
                return taskId;
              }
            },
            offloaded);

          edgeToDataFetcherMap.put(edge.getId(), fe);

          sourceVertexDataFetchers.add(fe);
          allFetchers.add(fe);

          if (sourceVertexDataFetchers.size() > 1) {
            throw new RuntimeException("Source vertex data fetcher is larger than one");
          }
        }
      });

    LOG.info("End of source vertex prepare {}", taskId);

      // Parent-task read
      // TODO #285: Cache broadcasted data
      task.getTaskIncomingEdges()
        .stream()
        // .filter(inEdge -> inEdge.getDstIRVertex().getId().equals(irVertex.getId())) // edge to this vertex
        .map(incomingEdge -> {

          LOG.info("Incoming edge: {}, taskIndex: {}, taskId: {}", incomingEdge, taskIndex, taskId);

          return Pair.of(incomingEdge, intermediateDataIOFactory
            .createReader(
              taskId,
              incomingEdge.getSrcIRVertex(), incomingEdge, executorThreadQueue));
        })
        .forEach(pair -> {
          final String irVertexId = pair.left().getDstIRVertex().getId();
          final IRVertex irVertex = irVertexDag.getVertexById(irVertexId);

          if (irVertex instanceof OperatorVertex) {

            // LOG.info("Adding data fetcher for {} / {}", taskId, irVertex.getId());

            final StageEdge edge = pair.left();
            final InputReader parentTaskReader = pair.right();
            final OutputCollector dataFetcherOutputCollector =
              new DataFetcherOutputCollector(edge.getSrcIRVertex(), (OperatorVertex) irVertex,
                outputCollectorMap.get(irVertex.getId()), taskId);

            final int parallelism = edge
              .getSrcIRVertex().getPropertyValue(ParallelismProperty.class).get();

            final CommunicationPatternProperty.Value comm =
              edge.getPropertyValue(CommunicationPatternProperty.class).get();

            final DataFetcher df = new MultiThreadParentTaskDataFetcher(
              taskId,
              edge.getSrcIRVertex(),
              edge,
              dataFetcherOutputCollector);

            edgeToDataFetcherMap.put(edge.getId(), df);

            // LOG.info("Adding data fetcher 22 for {} / {}, parallelism {}",
            //  taskId, irVertex.getId(), parallelism);

            LOG.info("Registering pipe for input edges in {}, parallelism {}", taskId, parallelism);

            if (comm.equals(CommunicationPatternProperty.Value.OneToOne)
              || comm.equals(CommunicationPatternProperty.Value.TransientOneToOne)) {
              inputPipeRegister.registerInputPipe(
                RuntimeIdManager.generateTaskId(edge.getSrc().getId(), taskIndex, 0),
                edge.getId(),
                task.getTaskId(),
                parentTaskReader);

              // LOG.info("Adding data fetcher 33 for {} / {}", taskId, irVertex.getId());
              taskWatermarkManager.addDataFetcher(df.getEdgeId(), 1);
            } else {
              for (int i = 0; i < parallelism; i++) {
                inputPipeRegister.registerInputPipe(
                  RuntimeIdManager.generateTaskId(edge.getSrc().getId(), i, 0),
                  edge.getId(),
                  task.getTaskId(),
                  parentTaskReader);
              }
              // LOG.info("Adding data fetcher 44 for {} / {}", taskId, irVertex.getId());
              taskWatermarkManager.addDataFetcher(df.getEdgeId(), parallelism);
            }


            allFetchers.add(df);

            // LOG.info("End of adding data fetcher for {} / {}", taskId, irVertex.getId());
          }
        });
    // return sortedHarnessList;
  }
  */


  public static void prepareTransform(final IRVertex irVertex,
                                      final Transform.Context context,
                                      final OutputCollector outputCollector,
                                      final String taskId) {
    final Transform transform;
    if (irVertex instanceof OperatorVertex) {
      transform = ((OperatorVertex) irVertex).getTransform();
      transform.prepare(context, outputCollector);
      // LOG.info("Preparing dofnTransform for {}/{}/{}", irVertex, taskId, transform);
    }
  }

  public static Optional<Readable> getSourceVertexReader(final IRVertex irVertex,
                                                  final Map<String, Readable> irVertexIdToReadable) {
    if (irVertex instanceof SourceVertex) {
      final Readable readable = irVertexIdToReadable.get(irVertex.getId());
      return Optional.of(readable);
    } else {
      return Optional.empty();
    }
  }


  /**
   * This wraps the encoder with OffloadingEventEncoder.
   * If the encoder is BytesEncoderFactory, we do not wrap the encoder.
   * TODO #276: Add NoCoder property value in Encoder/DecoderProperty
   * @param encoderFactory encoder factory
   * @return wrapped encoder
   */
  public static EncoderFactory getEncoderFactory(final EncoderFactory encoderFactory) {
    if (encoderFactory instanceof BytesEncoderFactory) {
      return encoderFactory;
    } else {
      return new NemoEventEncoderFactory(encoderFactory);
    }
  }

  /**
   * This wraps the encoder with OffloadingEventDecoder.
   * If the decoder is BytesDecoderFactory, we do not wrap the decoder.
   * TODO #276: Add NoCoder property value in Encoder/DecoderProperty
   * @param decoderFactory decoder factory
   * @return wrapped decoder
   */
  public static DecoderFactory getDecoderFactory(final DecoderFactory decoderFactory) {
    if (decoderFactory instanceof BytesDecoderFactory) {
      return decoderFactory;
    } else {
      return new NemoEventDecoderFactory(decoderFactory);
    }
  }


  /**
   * Return a map of Internal Outputs associated with their output tag.
   * If an edge has no output tag, its info are added to the mainOutputTag.
   *
   * @param irVertex source irVertex
   * @param irVertexDag DAG of IRVertex and RuntimeEdge
   * @param edgeIndexMap Map of edge and index
   * @return Map<OutputTag, List<NextIntraTaskOperatorInfo>>
   */
  public static Map<String, List<NextIntraTaskOperatorInfo>> getInternalOutputMap(
    final IRVertex irVertex,
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag) {
    // Add all intra-task tags to additional output map.
    final Map<String, List<NextIntraTaskOperatorInfo>> map = new HashMap<>();

    irVertexDag.getOutgoingEdgesOf(irVertex.getId())
      .stream()
      .map(edge -> {
        final boolean isPresent = edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent();
        final String outputTag;
        if (isPresent) {
          outputTag = edge.getPropertyValue(AdditionalOutputTagProperty.class).get();
        } else {
          outputTag = AdditionalOutputTagProperty.getMainOutputTag();
        }
        final OperatorVertex nextOperator = (OperatorVertex) edge.getDst();
        return Pair.of(outputTag, new NextIntraTaskOperatorInfo(edge, nextOperator));
      })
      .forEach(pair -> {
        map.putIfAbsent(pair.left(), new ArrayList<>());
        map.get(pair.left()).add(pair.right());
      });

    return map;
  }

  // Get all of the intra-task edges + inter-task edges
  public static List<Edge> getAllIncomingEdges(
    final Task task,
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
    final IRVertex childVertex) {
    final List<Edge> edges = new ArrayList<>();
    edges.addAll(irVertexDag.getIncomingEdgesOf(childVertex));
    final List<StageEdge> taskEdges = task.getTaskIncomingEdges().stream()
      .filter(edge -> edge.getDstIRVertex().getId().equals(childVertex.getId()))
      .collect(Collectors.toList());
    edges.addAll(taskEdges);
    return edges;
  }


  public static boolean hasExternalOutput(final IRVertex irVertex,
                                          final List<StageEdge> outEdgesToChildrenTasks) {
    final List<StageEdge> out =
    outEdgesToChildrenTasks
      .stream()
      .filter(edge -> edge.getSrcIRVertex().getId().equals(irVertex.getId()))
      .collect(Collectors.toList());

    return !out.isEmpty();
  }

  /**
   * Return inter-task OutputWriters, for single output or output associated with main tag.
   *
   * @param irVertex                source irVertex
   * @param outEdgesToChildrenTasks outgoing edges to child tasks
   * @param intermediateDataIOFactory     intermediateDataIOFactory
   * @return OutputWriters for main children tasks
   */
  public static List<OutputWriter> getExternalMainOutputs(final IRVertex irVertex,
                                                          final List<StageEdge> outEdgesToChildrenTasks,
                                                          final IntermediateDataIOFactory intermediateDataIOFactory,
                                                          final String taskId,
                                                          final TaskMetrics taskMetrics) {
    return outEdgesToChildrenTasks
      .stream()
      .filter(edge -> edge.getSrcIRVertex().getId().equals(irVertex.getId()))
      .filter(edge -> !edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(outEdgeForThisVertex -> {
        LOG.info("Set expected watermark map for vertex {}", outEdgeForThisVertex);

        final OutputWriter outputWriter;
        if (isPipe(outEdgeForThisVertex)) {
          outputWriter = intermediateDataIOFactory
            .createPipeWriter(
              taskId, outEdgeForThisVertex, taskMetrics);
        } else {
          outputWriter = intermediateDataIOFactory
            .createWriter(taskId, outEdgeForThisVertex);
        }
        return outputWriter;
      })
      .collect(Collectors.toList());
  }


  private static boolean isPipe(final RuntimeEdge runtimeEdge) {
    final Optional<DataStoreProperty.Value> dataStoreProperty = runtimeEdge.getPropertyValue(DataStoreProperty.class);
    return dataStoreProperty.isPresent() && dataStoreProperty.get().equals(DataStoreProperty.Value.Pipe);
  }

  ////////////////////////////////////////////// Helper methods for setting up initial data structures
  public static Map<String, List<OutputWriter>> getExternalAdditionalOutputMap(
    final IRVertex irVertex,
    final List<StageEdge> outEdgesToChildrenTasks,
    final IntermediateDataIOFactory intermediateDataIOFactory,
    final String taskId,
    final TaskMetrics taskMetrics) {
    // Add all inter-task additional tags to additional output map.
    final Map<String, List<OutputWriter>> map = new HashMap<>();

    outEdgesToChildrenTasks
      .stream()
      .filter(edge -> edge.getSrcIRVertex().getId().equals(irVertex.getId()))
      .filter(edge -> edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(edge -> {
        final OutputWriter outputWriter;
        LOG.info("Set expected watermark map for vertex {}", edge);

        if (isPipe(edge)) {
          outputWriter = intermediateDataIOFactory
            .createPipeWriter(taskId, edge, taskMetrics);
        } else {
          outputWriter = intermediateDataIOFactory
            .createWriter(taskId, edge);
        }

        final Pair<String, OutputWriter> pair =
        Pair.of(edge.getPropertyValue(AdditionalOutputTagProperty.class).get(), outputWriter);
        return pair;
      })
      .forEach(pair -> {
        map.putIfAbsent(pair.left(), new ArrayList<>());
        map.get(pair.left()).add(pair.right());
      });

    return map;
  }
}
