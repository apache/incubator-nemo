/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.vortex.compiler.optimizer.passes;

import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.common.dag.DAGBuilder;
import edu.snu.vortex.compiler.ir.MetricCollectionBarrierVertex;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.OperatorVertex;
import edu.snu.vortex.compiler.ir.attribute.Attribute;

import java.util.ArrayList;
import java.util.List;

/**
 * Pass to modify the DAG for the job to perform data skew.
 * It adds a {@link MetricCollectionBarrierVertex} before performing GroupByKey transform, to make a barrier before it,
 * and to use the metrics to repartition the skewed data.
 * NOTE: we currently put the DataSkewPass at the end of the list for each policies, as it needs to take a snapshot at
 * the end of the pass. This could be prevented by modifying other passes to take the snapshot of the DAG at the end of
 * each passes for metricCollectionVertices.
 */
public final class DataSkewPass implements StaticOptimizationPass {
  @Override
  public DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag) throws Exception {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    final List<MetricCollectionBarrierVertex> metricCollectionVertices = new ArrayList<>();

    dag.topologicalDo(v -> {
      // We care about OperatorVertices that have any incoming edges that are of type ScatterGather.
      if (v instanceof OperatorVertex && dag.getIncomingEdgesOf(v).stream().anyMatch(irEdge ->
          irEdge.getType().equals(IREdge.Type.ScatterGather))) {
        final MetricCollectionBarrierVertex metricCollectionBarrierVertex = new MetricCollectionBarrierVertex();
        metricCollectionBarrierVertex.setAttr(Attribute.Key.DynamicOptimizationType, Attribute.DataSkew);
        metricCollectionVertices.add(metricCollectionBarrierVertex);
        builder.addVertex(v);
        builder.addVertex(metricCollectionBarrierVertex);
        // We use memory for just a single inEdge, to make use of locality of stages: {@link PhysicalPlanGenerator}.
        final IREdge edgeToUseMemory = dag.getIncomingEdgesOf(v).stream().findFirst().orElseThrow(() ->
            new RuntimeException("This GroupByKey operator doesn't have any incoming edges: " + v.getId()));
        dag.getIncomingEdgesOf(v).forEach(edge -> {
          // We then insert the dynamicOptimizationVertex between the vertex and incoming vertices.
          final IREdge newEdge =
              new IREdge(IREdge.Type.OneToOne, edge.getSrc(), metricCollectionBarrierVertex, edge.getCoder());
          // we tell the edge that it needs to collect the metrics when transferring data.
          // we want it to be in the same stage
          newEdge.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);
          if (edge.equals(edgeToUseMemory)) {
            newEdge.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
          } else {
            newEdge.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.LocalFile);
          }

          final IREdge edgeToGbK = new IREdge(edge.getType(), metricCollectionBarrierVertex, v, edge.getCoder());
          IREdge.copyAttributes(edge, edgeToGbK);
          edgeToGbK.setAttr(Attribute.Key.DataSizeMetricCollection, Attribute.MetricCollection);
          builder.connectVertices(newEdge);
          builder.connectVertices(edgeToGbK);
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
}
