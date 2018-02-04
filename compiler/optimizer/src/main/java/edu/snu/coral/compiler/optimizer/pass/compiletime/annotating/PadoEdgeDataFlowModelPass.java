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
package edu.snu.coral.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.coral.common.dag.DAG;
import edu.snu.coral.common.ir.edge.IREdge;
import edu.snu.coral.common.ir.vertex.IRVertex;
import edu.snu.coral.common.ir.executionproperty.ExecutionProperty;
import edu.snu.coral.common.ir.edge.executionproperty.DataFlowModelProperty;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static edu.snu.coral.compiler.optimizer.pass.compiletime.annotating.PadoEdgeDataStorePass.fromTransientToReserved;

/**
 * Pado pass for tagging edges with DataFlowModel ExecutionProperty.
 */
public final class PadoEdgeDataFlowModelPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public PadoEdgeDataFlowModelPass() {
    super(ExecutionProperty.Key.DataFlowModel, Stream.of(
        ExecutionProperty.Key.ExecutorPlacement
    ).collect(Collectors.toSet()));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      if (!inEdges.isEmpty()) {
        inEdges.forEach(edge -> {
          if (fromTransientToReserved(edge)) {
            edge.setProperty(DataFlowModelProperty.of(DataFlowModelProperty.Value.Push));
          } else {
            edge.setProperty(DataFlowModelProperty.of(DataFlowModelProperty.Value.Pull));
          }
        });
      }
    });
    return dag;
  }
}
