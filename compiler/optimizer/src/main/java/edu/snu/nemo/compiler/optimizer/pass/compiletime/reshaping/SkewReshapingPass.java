/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.compiler.optimizer.pass.compiletime.reshaping;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.*;
import edu.snu.nemo.common.ir.vertex.*;
import edu.snu.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DecoderProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.EncoderProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.MetricCollectionVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.ArrayList;
import java.util.List;

/**
 * Pass to modify the DAG for a job to perform data skew.
 * It adds a {@link AggregationBarrierVertex} before Shuffle edges, to make a barrier before it,
 * and to use the metrics to repartition the skewed data.
 * NOTE: we currently put the SkewCompositePass at the end of the list for each policies, as it needs to take
 * a snapshot at the end of the pass. This could be prevented by modifying other passes to take the snapshot of the
 * DAG at the end of each passes for metricCollectionVertices.
 */
@Requires(CommunicationPatternProperty.class)
public final class SkewReshapingPass extends ReshapingPass {
  /**
   * Default constructor.
   */
  public SkewReshapingPass() {
    super(SkewReshapingPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    final List<MetricCollectionVertex> metricCollectionVertices = new ArrayList<>();

    dag.topologicalDo(v -> {
      // We care about OperatorVertices that have any incoming edges that are of type Shuffle.
      if (v instanceof OperatorVertex && dag.getIncomingEdgesOf(v).stream().anyMatch(irEdge ->
          CommunicationPatternProperty.Value.Shuffle
          .equals(irEdge.getPropertyValue(CommunicationPatternProperty.class).get()))) {
        final AggregationBarrierVertex<Integer, Long> aggregationBarrierVertex
            = new AggregationBarrierVertex<>();
        final SkewMetricCollectionVertex<Integer, Long> metricCollectionVertex
          = new SkewMetricCollectionVertex<>();

        metricCollectionVertices.add(metricCollectionVertex);
        builder.addVertex(v);
        builder.addVertex(metricCollectionVertex);
        dag.getIncomingEdgesOf(v).forEach(edge -> {
          // we insert the metric collection vertex when we meet a shuffle edge
          if (CommunicationPatternProperty.Value.Shuffle
                .equals(edge.getPropertyValue(CommunicationPatternProperty.class).get())) {
            // We then insert the dynamicOptimizationVertex between the vertex and incoming vertices.
            final IREdge edgeToMCV = generateEdgeToMCV(edge, metricCollectionVertex);
            final IREdge edgeToABV = generateEdgeToAGV(edge, metricCollectionVertex, aggregationBarrierVertex);
            final IREdge edgeToOriginalV = new IREdge(edge.getPropertyValue(CommunicationPatternProperty.class).get(),
                aggregationBarrierVertex, v, edge.isSideInput());
            edge.copyExecutionPropertiesTo(edgeToOriginalV);

            builder.connectVertices(edgeToMCV);
            builder.connectVertices(edgeToABV);
            builder.connectVertices(edgeToOriginalV);
          } else {
            builder.connectVertices(edge);
          }
        });
      } else { // Others are simply added to the builder, unless it comes from an updated vertex
        builder.addVertex(v);
        dag.getIncomingEdgesOf(v).forEach(builder::connectVertices);
      }
    });
    final DAG<IRVertex, IREdge> newDAG = builder.build();
    metricCollectionVertices.forEach(v -> v.setDAGSnapshot(newDAG));
    return newDAG;
  }
  
  private IREdge generateEdgeToMCV(final IREdge edge, final SkewMetricCollectionVertex mcv) {
    final IREdge newEdge = new IREdge(CommunicationPatternProperty.Value.OneToOne,
      edge.getSrc(), mcv);
    newEdge.setProperty(EncoderProperty.of(edge.getPropertyValue(EncoderProperty.class).get()));
    newEdge.setProperty(DecoderProperty.of(edge.getPropertyValue(DecoderProperty.class).get()));
    return newEdge;
  }
  
  private IREdge generateEdgeToAGV(final IREdge edge,
                                   final SkewMetricCollectionVertex mcv,
                                   final AggregationBarrierVertex agv) {
    final IREdge newEdge = new IREdge(CommunicationPatternProperty.Value.OneToOne, mcv, agv);
    newEdge.setProperty(DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore));
    newEdge.setProperty(DataFlowProperty.of(DataFlowProperty.Value.Push));
    newEdge.setProperty(EncoderProperty.of(edge.getPropertyValue(EncoderProperty.class).get()));
    newEdge.setProperty(DecoderProperty.of(edge.getPropertyValue(DecoderProperty.class).get()));
    return newEdge;
  }
}
