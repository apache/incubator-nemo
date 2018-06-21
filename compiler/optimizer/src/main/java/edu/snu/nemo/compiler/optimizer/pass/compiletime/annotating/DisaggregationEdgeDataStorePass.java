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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.InterTaskDataStoreProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.dag.DAG;

import java.util.Collections;
import java.util.List;

/**
 * A pass to support Disaggregated Resources by tagging edges.
 * This pass handles the DataStore ExecutionProperty.
 */
public final class DisaggregationEdgeDataStorePass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public DisaggregationEdgeDataStorePass() {
    super(InterTaskDataStoreProperty.class, Collections.singleton(InterTaskDataStoreProperty.class));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> { // Initialize the DataStore of the DAG with GlusterFileStore.
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      inEdges.forEach(edge -> {
        edge.setProperty(InterTaskDataStoreProperty.of(InterTaskDataStoreProperty.Value.GlusterFileStore));
      });
    });
    return dag;
  }
}
