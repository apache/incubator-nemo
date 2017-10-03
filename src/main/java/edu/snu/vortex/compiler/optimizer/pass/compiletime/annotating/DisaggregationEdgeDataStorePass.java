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

import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.ir.executionproperty.edge.DataStoreProperty;
import edu.snu.vortex.runtime.executor.data.GlusterFileStore;
import edu.snu.vortex.runtime.executor.data.MemoryStore;
import edu.snu.vortex.runtime.executor.datatransfer.data_communication_pattern.OneToOne;

import java.util.List;

/**
 * A pass to support Disaggregated Resources by tagging edges.
 * This pass handles the DataStore ExecutionProperty.
 */
public final class DisaggregationEdgeDataStorePass extends AnnotatingPass {
  public static final String SIMPLE_NAME = "DisaggregationEdgeDataStorePass";

  public DisaggregationEdgeDataStorePass() {
    super(ExecutionProperty.Key.DataStore);
  }

  @Override
  public String getName() {
    return SIMPLE_NAME;
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      if (!inEdges.isEmpty()) {
        inEdges.forEach(edge -> {
          if (OneToOne.class.equals(edge.getProperty(ExecutionProperty.Key.DataCommunicationPattern))) {
            edge.setProperty(DataStoreProperty.of(MemoryStore.class));
          } else {
            edge.setProperty(DataStoreProperty.of(GlusterFileStore.class));
          }
        });
      }
    });
    return dag;
  }
}
