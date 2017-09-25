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
package edu.snu.vortex.compiler.optimizer.pass;

import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.ir.executionproperty.edge.DataFlowModelProperty;
import edu.snu.vortex.compiler.ir.executionproperty.edge.DataStoreProperty;
import edu.snu.vortex.compiler.ir.executionproperty.vertex.ExecutorPlacementProperty;
import edu.snu.vortex.runtime.executor.data.LocalFileStore;
import edu.snu.vortex.runtime.executor.data.MemoryStore;

import java.util.List;

/**
 * Pado pass for tagging edges.
 */
public final class PadoEdgePass implements StaticOptimizationPass {
  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      if (!inEdges.isEmpty()) {
        inEdges.forEach(edge -> {
          if (fromTransientToReserved(edge)) {
            edge.setProperty(DataStoreProperty.of(LocalFileStore.class));
            edge.setProperty(DataFlowModelProperty.of(DataFlowModelProperty.Value.Push));
          } else if (fromReservedToTransient(edge)) {
            edge.setProperty(DataStoreProperty.of(LocalFileStore.class));
            edge.setProperty(DataFlowModelProperty.of(DataFlowModelProperty.Value.Pull));
          } else {
            if (edge.getType().equals(IREdge.Type.OneToOne)) {
              edge.setProperty(DataStoreProperty.of(MemoryStore.class));
              edge.setProperty(DataFlowModelProperty.of(DataFlowModelProperty.Value.Pull));
            } else {
              edge.setProperty(DataStoreProperty.of(LocalFileStore.class));
              edge.setProperty(DataFlowModelProperty.of(DataFlowModelProperty.Value.Pull));
            }
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
  private static boolean fromTransientToReserved(final IREdge irEdge) {
    return irEdge.getSrc().get(ExecutionProperty.Key.ExecutorPlacement)
        .equals(ExecutorPlacementProperty.TRANSIENT)
        && irEdge.getDst().get(ExecutionProperty.Key.ExecutorPlacement)
        .equals(ExecutorPlacementProperty.RESERVED);
  }

  /**
   * checks if the edge is from reserved container to a transient container.
   * @param irEdge edge to check.
   * @return whether or not the edge satisfies the condition.
   */
  private static boolean fromReservedToTransient(final IREdge irEdge) {
    return irEdge.getSrc().get(ExecutionProperty.Key.ExecutorPlacement)
        .equals(ExecutorPlacementProperty.RESERVED)
        && irEdge.getDst().get(ExecutionProperty.Key.ExecutorPlacement)
        .equals(ExecutorPlacementProperty.TRANSIENT);
  }
}
