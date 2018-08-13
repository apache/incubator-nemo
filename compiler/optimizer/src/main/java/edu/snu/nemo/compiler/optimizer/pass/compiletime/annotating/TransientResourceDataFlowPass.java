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
import edu.snu.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;

import java.util.Collections;
import java.util.List;

import static edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.TransientResourceDataStorePass.fromTransientToReserved;

/**
 * Push from transient resources to reserved resources.
 */
public final class TransientResourceDataFlowPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public TransientResourceDataFlowPass() {
    super(DataFlowProperty.class, Collections.singleton(ResourcePriorityProperty.class));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      if (!inEdges.isEmpty()) {
        inEdges.forEach(edge -> {
          if (fromTransientToReserved(edge)) {
            edge.setPropertyPermanently(DataFlowProperty.of(DataFlowProperty.Value.Push));
          } else {
            edge.setPropertyPermanently(DataFlowProperty.of(DataFlowProperty.Value.Pull));
          }
        });
      }
    });
    return dag;
  }
}
