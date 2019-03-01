package org.apache.nemo.compiler.frontend.beam.transform;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.nemo.common.*;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.eventhandler.OffloadingDataEvent;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.edge.executionproperty.AdditionalOutputTagProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.offloading.common.OffloadingOutputCollector;
import org.apache.nemo.offloading.common.OffloadingTransform;

import java.util.*;
import java.util.stream.Collectors;

public final class StatelessOffloadingTransform<O> implements OffloadingTransform<OffloadingDataEvent, O> {

  private final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag;

  // key: data fetcher id, value: head operator
  private final Map<String, OffloadingOperatorVertexOutputCollector> srcOutputCollectorMap;

  private transient OffloadingResultCollector resultCollector;


  public StatelessOffloadingTransform(final DAG<IRVertex, RuntimeEdge<IRVertex>> irDag) {
    this.irDag = irDag;
    this.srcOutputCollectorMap = new HashMap<>();
  }

  @Override
  public void prepare(final OffloadingContext context,
                      final OffloadingOutputCollector oc) {
    System.out.println("Stateless offloading transform prepare");
    // Traverse in a reverse-topological order to ensure that each visited vertex's children vertices exist.
    final List<IRVertex> reverseTopologicallySorted = Lists.reverse(irDag.getTopologicalSort());
    resultCollector = new OffloadingResultCollector(oc);

    // Build a map for edge as a key and edge index as a value
    // This variable is used for creating NextIntraTaskOperatorInfo
    // in {@link this#getInternalMainOutputs and this#internalMainOutputs}
    final Map<Edge, Integer> edgeIndexMap = new HashMap<>();
    reverseTopologicallySorted.forEach(childVertex -> {
      final List<Edge> edges = getAllIncomingEdges(irDag, childVertex);
      for (int edgeIndex = 0; edgeIndex < edges.size(); edgeIndex++) {
        final Edge edge = edges.get(edgeIndex);
        edgeIndexMap.putIfAbsent(edge, edgeIndex);
      }
    });

    // Build a map for InputWatermarkManager for each operator vertex
    // This variable is used for creating NextIntraTaskOperatorInfo
    // in {@link this#getInternalMainOutputs and this#internalMainOutputs}
    final Map<IRVertex, InputWatermarkManager> operatorWatermarkManagerMap = new HashMap<>();
    reverseTopologicallySorted.forEach(childVertex -> {

      if (childVertex instanceof OperatorVertex) {
        final List<Edge> edges = getAllIncomingEdges(irDag, childVertex);
        if (edges.size() == 1) {
          operatorWatermarkManagerMap.putIfAbsent(childVertex,
            new SingleInputWatermarkManager(
              new OperatorWatermarkCollector((OperatorVertex) childVertex)));
        } else {
          operatorWatermarkManagerMap.putIfAbsent(childVertex,
            new MultiInputWatermarkManager(edges.size(),
              new OperatorWatermarkCollector((OperatorVertex) childVertex)));
        }
      }
    });

    reverseTopologicallySorted.forEach(irVertex -> {

      // Additional outputs
      final Map<String, List<NextIntraTaskOperatorInfo>> internalAdditionalOutputMap =
        getInternalAdditionalOutputMap(irVertex, irDag, edgeIndexMap, operatorWatermarkManagerMap);

      // Main outputs
      final List<NextIntraTaskOperatorInfo> internalMainOutputs =
        getInternalMainOutputs(irVertex, irDag, edgeIndexMap, operatorWatermarkManagerMap);

      final boolean isSink = irVertex.isSink;
      // skip sink
      if (!isSink) {
        System.out.println("vertex " + irVertex.getId() + " outgoing edges: " + irDag.getOutgoingEdgesOf(irVertex)
          + ", isSink: " + isSink);
        OffloadingOperatorVertexOutputCollector outputCollector = new OffloadingOperatorVertexOutputCollector(
          irVertex, irDag.getOutgoingEdgesOf(irVertex).get(0), /* just use first edge for encoding */
          internalMainOutputs, internalAdditionalOutputMap, resultCollector);

        // add source
        if (irDag.getRootVertices().contains(irVertex)) {
          srcOutputCollectorMap.put(irVertex.getId(), outputCollector);
        }

        final Transform transform;
        if (irVertex instanceof OperatorVertex) {
          transform = ((OperatorVertex) irVertex).getTransform();
          transform.prepare(new OffloadingTransformContextImpl(irVertex), outputCollector);
        }
      }
    });
  }

  // receive batch (list) data
  @Override
  public void onData(final OffloadingDataEvent element) {
    for (final Pair<String, Object> input : element.data) {
      final String srcId = input.left();
      final Object d = input.right();
      final OffloadingOperatorVertexOutputCollector outputCollector = srcOutputCollectorMap.get(srcId);
      if (d instanceof Watermark) {
        outputCollector.emitWatermark((Watermark) d);
      } else {
        outputCollector.emit(d);
      }
    }

    resultCollector.flush();
  }

  @Override
  public void close() {

  }



  // Get all of the intra-task edges
  private static List<Edge> getAllIncomingEdges(
    final DAG<IRVertex, RuntimeEdge<IRVertex>> irVertexDag,
    final IRVertex childVertex) {
    final List<Edge> edges = new ArrayList<>();
    edges.addAll(irVertexDag.getIncomingEdgesOf(childVertex));
    return edges;
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

  // TODO #253: Refactor getInternal(Main/Additional)OutputMap
  private static Map<String, List<NextIntraTaskOperatorInfo>> getInternalAdditionalOutputMap(
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
}
