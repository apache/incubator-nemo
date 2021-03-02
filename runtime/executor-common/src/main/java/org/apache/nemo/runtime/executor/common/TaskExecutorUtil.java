package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.TaskMetrics;
import org.apache.nemo.common.coder.BytesDecoderFactory;
import org.apache.nemo.common.coder.BytesEncoderFactory;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.common.Task;
import org.apache.nemo.runtime.executor.common.datatransfer.OutputWriter;
import org.apache.nemo.runtime.executor.common.datatransfer.IntermediateDataIOFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public final class TaskExecutorUtil {
  private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorUtil.class.getName());

  public static void prepareTransform(final IRVertex irVertex,
                                      final Transform.Context context,
                                      final OutputCollector outputCollector,
                                      final String taskId) {
    final Transform transform;
    if (irVertex instanceof OperatorVertex) {
      transform = ((OperatorVertex) irVertex).getTransform();
      transform.prepare(context, outputCollector);
      LOG.info("Preparing dofnTransform for {}/{}/{}", irVertex, taskId, transform);
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
