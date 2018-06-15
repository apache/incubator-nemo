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

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Edge data store pass to process inter-stage memory store edges.
 */
public final class ReviseInterStageEdgeDataStorePass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public ReviseInterStageEdgeDataStorePass() {
    super(ExecutionProperty.Key.DataStore, Stream.of(
        ExecutionProperty.Key.StageId
    ).collect(Collectors.toSet()));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      if (!inEdges.isEmpty()) {
        inEdges.forEach(edge -> {
          if (DataStoreProperty.Value.MemoryStore.equals(edge.getProperty(ExecutionProperty.Key.DataStore))
              && !edge.getSrc().getProperty(ExecutionProperty.Key.StageId)
              .equals(edge.getDst().getProperty(ExecutionProperty.Key.StageId))) {
            edge.setProperty(DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore));
          }
        });
      }
    });
    return dag;
  }
}
