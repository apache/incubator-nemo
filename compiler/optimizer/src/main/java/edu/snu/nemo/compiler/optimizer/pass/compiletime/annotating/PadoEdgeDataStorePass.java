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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorPlacementProperty;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Pado pass for tagging edges with DataStore ExecutionProperty.
 */
public final class PadoEdgeDataStorePass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public PadoEdgeDataStorePass() {
    super(ExecutionProperty.Key.DataStore, Stream.of(
        ExecutionProperty.Key.ExecutorPlacement
    ).collect(Collectors.toSet()));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      if (!inEdges.isEmpty()) {
        inEdges.forEach(edge -> {
          if (fromTransientToReserved(edge) || fromReservedToTransient(edge)) {
            edge.setProperty(DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore));
          } else if (DataCommunicationPatternProperty.Value.OneToOne
              .equals(edge.getProperty(ExecutionProperty.Key.DataCommunicationPattern))) {
            edge.setProperty(DataStoreProperty.of(DataStoreProperty.Value.MemoryStore));
          } else {
            edge.setProperty(DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore));
          }
        });
      }
    });
    return dag;
  }

  /**
   * checks if the edge is from transient container to a reserved container.
   * @param irEdge edge to check.
   * @return whether or not the edge satisfies the condition.
   */
  static boolean fromTransientToReserved(final IREdge irEdge) {
    return ExecutorPlacementProperty.TRANSIENT
        .equals(irEdge.getSrc().getProperty(ExecutionProperty.Key.ExecutorPlacement))
        && ExecutorPlacementProperty.RESERVED
        .equals(irEdge.getDst().getProperty(ExecutionProperty.Key.ExecutorPlacement));
  }

  /**
   * checks if the edge is from reserved container to a transient container.
   * @param irEdge edge to check.
   * @return whether or not the edge satisfies the condition.
   */
  static boolean fromReservedToTransient(final IREdge irEdge) {
    return ExecutorPlacementProperty.RESERVED
        .equals(irEdge.getSrc().getProperty(ExecutionProperty.Key.ExecutorPlacement))
        && ExecutorPlacementProperty.TRANSIENT
        .equals(irEdge.getDst().getProperty(ExecutionProperty.Key.ExecutorPlacement));
  }
}
