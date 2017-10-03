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
import edu.snu.vortex.compiler.ir.executionproperty.vertex.ExecutorPlacementProperty;
import edu.snu.vortex.runtime.executor.datatransfer.data_communication_pattern.ScatterGather;

import java.util.List;

/**
 * Pado pass for tagging vertices.
 */
public final class PadoVertexPass extends AnnotatingPass {
  public static final String SIMPLE_NAME = "PadoVertexPass";

  public PadoVertexPass() {
    super(ExecutionProperty.Key.ExecutorPlacement);
  }

  @Override
  public String getName() {
    return SIMPLE_NAME;
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      if (inEdges.isEmpty()) {
        vertex.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.TRANSIENT));
      } else {
        if (hasM2M(inEdges) || allFromReserved(inEdges)) {
          vertex.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.RESERVED));
        } else {
          vertex.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.TRANSIENT));
        }
      }
    });
    return dag;
  }

  /**
   * Checks whether the irEdges have M2M relationship.
   * @param irEdges irEdges to check.
   * @return whether of not any of them has M2M relationship.
   */
  private boolean hasM2M(final List<IREdge> irEdges) {
    return irEdges.stream().anyMatch(edge ->
        ScatterGather.class.equals(edge.getProperty(ExecutionProperty.Key.DataCommunicationPattern)));
  }

  /**
   * Checks whether the irEdges are all from reserved containers.
   * @param irEdges irEdges to check.
   * @return whether of not they are from reserved containers.
   */
  private boolean allFromReserved(final List<IREdge> irEdges) {
    return irEdges.stream()
        .allMatch(edge ->
            edge.getSrc().getProperty(ExecutionProperty.Key.ExecutorPlacement)
                .equals(ExecutorPlacementProperty.RESERVED));
  }
}
