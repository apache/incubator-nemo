package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.common.ir.edge.StageEdge;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.executor.common.InputWatermarkManager;
import org.apache.nemo.runtime.executor.datatransfer.OutputWriter;
import org.apache.nemo.runtime.executor.datatransfer.IntermediateDataIOFactory;
import org.apache.nemo.runtime.executor.common.NextIntraTaskOperatorInfo;
import org.apache.nemo.common.ir.Readable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public final class TaskExecutorUtil {
  private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorUtil.class.getName());

  public static void prepareTransform(final VertexHarness vertexHarness) {
    final IRVertex irVertex = vertexHarness.getIRVertex();
    final Transform transform;
    if (irVertex instanceof OperatorVertex) {
      transform = ((OperatorVertex) irVertex).getTransform();
      transform.prepare(vertexHarness.getContext(), vertexHarness.getOutputCollector());
    }
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


  public static Optional<Readable> getSourceVertexReader(final IRVertex irVertex,
                                                         final Map<String, Readable> irVertexIdToReadable) {
    if (irVertex instanceof SourceVertex) {
      final Readable readable = irVertexIdToReadable.get(irVertex.getId());
      if (readable == null) {
        throw new IllegalStateException(irVertex.toString());
      }
      return Optional.of(readable);
    } else {
      return Optional.empty();
    }
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
                                                          final Map<String, OutputWriter> outputWriterMap,
                                                          final Map<String, Pair<PriorityQueue<Watermark>, PriorityQueue<Watermark>>> expectedWatermarkMap,
                                                          final Map<Long, Long> prevWatermarkMap,
                                                          final Map<Long, Integer> watermarkCounterMap) {
    return outEdgesToChildrenTasks
      .stream()
      .filter(edge -> edge.getSrcIRVertex().getId().equals(irVertex.getId()))
      .filter(edge -> !edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(outEdgeForThisVertex -> {
        LOG.info("Set expected watermark map for vertex {}", outEdgeForThisVertex);
        expectedWatermarkMap.put(outEdgeForThisVertex.getDstIRVertex().getId(), Pair.of(new PriorityQueue<>(), new PriorityQueue<>()));

        final OutputWriter outputWriter;
        if (isPipe(outEdgeForThisVertex)) {
          outputWriter = intermediateDataIOFactory
            .createPipeWriter(taskId, outEdgeForThisVertex, expectedWatermarkMap, prevWatermarkMap, watermarkCounterMap);
        } else {
          outputWriter = intermediateDataIOFactory
            .createWriter(taskId, outEdgeForThisVertex);
        }
        outputWriterMap.put(outEdgeForThisVertex.getDstIRVertex().getId(), outputWriter);
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
    final Map<String, OutputWriter> outputWriterMap,
    final Map<String, Pair<PriorityQueue<Watermark>, PriorityQueue<Watermark>>> expectedWatermarkMap,
    final Map<Long, Long> prevWatermarkMap,
    final Map<Long, Integer> watermarkCounterMap) {
    // Add all inter-task additional tags to additional output map.
    final Map<String, List<OutputWriter>> map = new HashMap<>();

    outEdgesToChildrenTasks
      .stream()
      .filter(edge -> edge.getSrcIRVertex().getId().equals(irVertex.getId()))
      .filter(edge -> edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(edge -> {
        final OutputWriter outputWriter;
        LOG.info("Set expected watermark map for vertex {}", edge);
        expectedWatermarkMap.put(edge.getDstIRVertex().getId(), Pair.of(new PriorityQueue<>(), new PriorityQueue<>()));

        if (isPipe(edge)) {
          outputWriter = intermediateDataIOFactory
            .createPipeWriter(taskId, edge, expectedWatermarkMap, prevWatermarkMap, watermarkCounterMap);
        } else {
          outputWriter = intermediateDataIOFactory
            .createWriter(taskId, edge);
        }

        final Pair<String, OutputWriter> pair =
        Pair.of(edge.getPropertyValue(AdditionalOutputTagProperty.class).get(), outputWriter);
        outputWriterMap.put(edge.getDstIRVertex().getId(), pair.right());
        return pair;
      })
      .forEach(pair -> {
        map.putIfAbsent(pair.left(), new ArrayList<>());
        map.get(pair.left()).add(pair.right());
      });

    return map;
  }




  // TODO #253: Refactor getInternal(Main/Additional)OutputMap
  public static Map<String, List<NextIntraTaskOperatorInfo>> getInternalAdditionalOutputMap(
    final IRVertex irVertex,
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
    final Map<Edge, Integer> edgeIndexMap,
    final Map<IRVertex, InputWatermarkManager> operatorWatermarkManagerMap) {
    // Add all intra-task additional tags to additional output map.
    final Map<String, List<NextIntraTaskOperatorInfo>> map = new HashMap<>();

    irVertexDag.getOutgoingEdgesOf(irVertex.getId())
      .stream()
      .filter(edge -> edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(edge -> {
        final String outputTag = edge.getPropertyValue(AdditionalOutputTagProperty.class).get();
        final int index = edgeIndexMap.get(edge);
        final OperatorVertex nextOperator = (OperatorVertex) edge.getDst();
        final InputWatermarkManager inputWatermarkManager = operatorWatermarkManagerMap.get(nextOperator);
        return Pair.of(outputTag, new NextIntraTaskOperatorInfo(index, edge, nextOperator, inputWatermarkManager));
      })
      .forEach(pair -> {
        map.putIfAbsent(pair.left(), new ArrayList<>());
        map.get(pair.left()).add(pair.right());
      });

    return map;
  }


  // TODO #253: Refactor getInternal(Main/Additional)OutputMap
  public static List<NextIntraTaskOperatorInfo> getInternalMainOutputs(
    final IRVertex irVertex,
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
    final Map<Edge, Integer> edgeIndexMap,
    final Map<IRVertex, InputWatermarkManager> operatorWatermarkManagerMap) {

    return irVertexDag.getOutgoingEdgesOf(irVertex.getId())
      .stream()
      .filter(edge -> !edge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())
      .map(edge -> {
        final int index = edgeIndexMap.get(edge);
        final OperatorVertex nextOperator = (OperatorVertex) edge.getDst();
        final InputWatermarkManager inputWatermarkManager = operatorWatermarkManagerMap.get(nextOperator);
        return new NextIntraTaskOperatorInfo(index, edge, nextOperator, inputWatermarkManager);
      })
      .collect(Collectors.toList());
  }
}
