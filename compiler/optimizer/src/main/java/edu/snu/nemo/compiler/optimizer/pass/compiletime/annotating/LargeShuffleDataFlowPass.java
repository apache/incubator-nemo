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
import edu.snu.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;

import java.util.Collections;
import java.util.List;

/**
 * A pass to optimize large shuffle by tagging edges.
 * This pass handles the DataFlowModel ExecutionProperty.
 */
public final class LargeShuffleDataFlowPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public LargeShuffleDataFlowPass() {
    super(DataFlowProperty.class, Collections.singleton(CommunicationPatternProperty.class));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      inEdges.forEach(edge -> {
        if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
            .equals(CommunicationPatternProperty.Value.Shuffle)) {
          edge.setProperty(DataFlowProperty.of(DataFlowProperty.Value.Push)); // Push to the merger vertex.
        } else {
          edge.setProperty(DataFlowProperty.of(DataFlowProperty.Value.Pull));
        }
      });
    });
    return dag;
  }
}
