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
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.ir.executionproperty.edge.DataFlowModelProperty;
import edu.snu.vortex.runtime.executor.datatransfer.communication.ScatterGather;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Pass for tagging shuffle edges with "Push" DataFlowModel ExecutionProperty.
 */
public final class PushDataFlowModelPass extends AnnotatingPass {
  public static final String SIMPLE_NAME = "PadoEdgeDataFlowModelPass";

  public PushDataFlowModelPass() {
    super(ExecutionProperty.Key.DataFlowModel, Stream.of(
        ExecutionProperty.Key.ExecutorPlacement
    ).collect(Collectors.toSet()));
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
          if (ScatterGather.class.equals(edge.getProperty(ExecutionProperty.Key.DataCommunicationPattern))) {
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
