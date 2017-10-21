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
import edu.snu.onyx.compiler.ir.IREdge;
import edu.snu.onyx.compiler.ir.IRVertex;
import edu.snu.onyx.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.compiler.ir.executionproperty.edge.DataStoreProperty;
import edu.snu.onyx.runtime.executor.data.GlusterFileStore;

import java.util.Collections;
import java.util.List;

/**
 * A pass to support Disaggregated Resources by tagging edges.
 * This pass handles the DataStore ExecutionProperty.
 */
public final class InterStageGlusterPass extends AnnotatingPass {
  public static final String SIMPLE_NAME = "InterStageGlusterPass";

  public InterStageGlusterPass() {
    super(ExecutionProperty.Key.DataStore, Collections.singleton(ExecutionProperty.Key.DataStore));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> { // Initialize the DataStore of the DAG with GlusterFileStore.
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      inEdges.forEach(edge -> {
        final IRVertex src = edge.getSrc();
        if (!src.getProperty(ExecutionProperty.Key.StageId).equals(vertex.getProperty(ExecutionProperty.Key.StageId))) {
          edge.setProperty(DataStoreProperty.of(GlusterFileStore.class));
        }
      });
    });
    return dag;
  }
}
