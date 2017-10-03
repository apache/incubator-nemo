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
package edu.snu.vortex.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.MetricCollectionBarrierVertex;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.ir.executionproperty.edge.DataStoreProperty;
import edu.snu.vortex.runtime.executor.data.LocalFileStore;
import edu.snu.vortex.runtime.executor.data.MemoryStore;

/**
 * Pass to annotate the DAG for a job to perform data skew.
 * It specifies the incoming one-to-one edges to MetricCollectionVertices to have either MemoryStore or LocalFileStore
 * as its DataStore ExecutionProperty.
 */
public final class DataSkewEdgeDataStorePass extends AnnotatingPass {
  public static final String SIMPLE_NAME = "DataSkewEdgeDataStorePass";

  public DataSkewEdgeDataStorePass() {
    super(ExecutionProperty.Key.DataStore);
  }

  @Override
  public String getName() {
    return SIMPLE_NAME;
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
            edge.setProperty(DataStoreProperty.of(MemoryStore.class));
          } else {
            edge.setProperty(DataStoreProperty.of(LocalFileStore.class));
          }
        });
      }
    });
    return dag;
  }
}
