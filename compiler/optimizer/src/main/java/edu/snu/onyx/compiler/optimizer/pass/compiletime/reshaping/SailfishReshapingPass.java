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
package edu.snu.onyx.compiler.optimizer.pass.compiletime.reshaping;

import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.dag.DAGBuilder;
import edu.snu.onyx.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.ir.vertex.OperatorVertex;
import edu.snu.onyx.compiler.frontend.beam.transform.RelayTransform;

/**
 * Pass to modify the DAG for a job to batch the disk seek.
 * It adds a {@link OperatorVertex} with {@link RelayTransform} before the vertices
 * receiving {@link edu.snu.onyx.runtime.executor.datatransfer.communication.ScatterGather} edges,
 * to merge the shuffled data in memory and write to the disk at once.
 */
public final class SailfishReshapingPass extends ReshapingPass {
  public static final String SIMPLE_NAME = "SailfishReshapingPass";

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    dag.topologicalDo(v -> {
      builder.addVertex(v);
      // We care about OperatorVertices that have any incoming edge that
      // has ScatterGather as data communication pattern.
      if (v instanceof OperatorVertex && dag.getIncomingEdgesOf(v).stream().anyMatch(irEdge ->
              DataCommunicationPatternProperty.Value.ScatterGather
          .equals(irEdge.getProperty(ExecutionProperty.Key.DataCommunicationPattern)))) {
        dag.getIncomingEdgesOf(v).forEach(edge -> {
          if (DataCommunicationPatternProperty.Value.ScatterGather
                .equals(edge.getProperty(ExecutionProperty.Key.DataCommunicationPattern))) {
            // Insert a merger vertex having transform that write received data immediately
            // before the vertex receiving shuffled data.
            final OperatorVertex iFileMergerVertex = new OperatorVertex(new RelayTransform());
            builder.addVertex(iFileMergerVertex);
            final IREdge newEdgeToMerger = new IREdge(DataCommunicationPatternProperty.Value.ScatterGather,
                edge.getSrc(), iFileMergerVertex, edge.getCoder(), edge.isSideInput());
            final IREdge newEdgeFromMerger = new IREdge(DataCommunicationPatternProperty.Value.OneToOne,
                iFileMergerVertex, v, edge.getCoder());
            edge.copyExecutionPropertiesTo(newEdgeToMerger);
            builder.connectVertices(newEdgeToMerger);
            builder.connectVertices(newEdgeFromMerger);
          } else {
            builder.connectVertices(edge);
          }
        });
      } else { // Others are simply added to the builder.
        dag.getIncomingEdgesOf(v).forEach(builder::connectVertices);
      }
    });
    return builder.build();
  }
}
