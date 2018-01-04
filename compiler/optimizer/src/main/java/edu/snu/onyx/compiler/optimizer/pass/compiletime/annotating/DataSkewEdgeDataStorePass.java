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
package edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.ir.vertex.MetricCollectionBarrierVertex;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.common.ir.edge.executionproperty.DataStoreProperty;

/**
 * Pass to annotate the DAG for a job to perform data skew.
 * It specifies the incoming one-to-one edges to MetricCollectionVertices to have either MemoryStore or LocalFileStore
 * as its DataStore ExecutionProperty.
 */
public final class DataSkewEdgeDataStorePass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public DataSkewEdgeDataStorePass() {
    super(ExecutionProperty.Key.DataStore);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(v -> {
      // we only care about metric collection barrier vertices.
      if (v instanceof MetricCollectionBarrierVertex) {
        // We use memory for just a single inEdge, to make use of locality of stages: {@link PhysicalPlanGenerator}.
        final IREdge edgeToUseMemory = dag.getIncomingEdgesOf(v).stream().findFirst().orElseThrow(() ->
            new RuntimeException("This MetricCollectionBarrierVertex doesn't have any incoming edges: " + v.getId()));
        dag.getIncomingEdgesOf(v).forEach(edge -> {
          // we want it to be in the same stage
          if (edge.equals(edgeToUseMemory)) {
            edge.setProperty(DataStoreProperty.of(DataStoreProperty.Value.MemoryStore));
          } else {
            edge.setProperty(DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore));
          }
        });
      }
    });
    return dag;
  }
}
