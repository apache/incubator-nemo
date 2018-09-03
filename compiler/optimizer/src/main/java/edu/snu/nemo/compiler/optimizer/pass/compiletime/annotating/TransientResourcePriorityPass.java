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
import edu.snu.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.List;

/**
 * Place valuable computations on reserved resources, and the rest on transient resources.
 */
@Annotates(ResourcePriorityProperty.class)
@Requires(CommunicationPatternProperty.class)
public final class TransientResourcePriorityPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public TransientResourcePriorityPass() {
    super(TransientResourcePriorityPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      if (inEdges.isEmpty()) {
        vertex.setPropertyPermanently(ResourcePriorityProperty.of(ResourcePriorityProperty.TRANSIENT));
      } else {
        if (hasM2M(inEdges) || allO2OFromReserved(inEdges)) {
          vertex.setPropertyPermanently(ResourcePriorityProperty.of(ResourcePriorityProperty.RESERVED));
        } else {
          vertex.setPropertyPermanently(ResourcePriorityProperty.of(ResourcePriorityProperty.TRANSIENT));
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
        edge.getPropertyValue(CommunicationPatternProperty.class).get()
          .equals(CommunicationPatternProperty.Value.Shuffle));
  }

  /**
   * Checks whether the irEdges are all from reserved containers.
   * @param irEdges irEdges to check.
   * @return whether of not they are from reserved containers.
   */
  private boolean allO2OFromReserved(final List<IREdge> irEdges) {
    return irEdges.stream()
        .allMatch(edge -> CommunicationPatternProperty.Value.OneToOne.equals(
            edge.getPropertyValue(CommunicationPatternProperty.class).get())
            && edge.getSrc().getPropertyValue(ResourcePriorityProperty.class).get().equals(
                ResourcePriorityProperty.RESERVED));
  }
}
