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
import edu.snu.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;

import java.util.Collections;
import java.util.List;

/**
 * Transient resource pass for tagging edges with DataStore ExecutionProperty.
 */
public final class TransientResourceDataStorePass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public TransientResourceDataStorePass() {
    super(DataStoreProperty.class, Collections.singleton(ResourcePriorityProperty.class));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      if (!inEdges.isEmpty()) {
        inEdges.forEach(edge -> {
          if (fromTransientToReserved(edge) || fromReservedToTransient(edge)) {
            edge.setProperty(DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore));
          } else if (CommunicationPatternProperty.Value.OneToOne
              .equals(edge.getPropertyValue(CommunicationPatternProperty.class).get())) {
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
    return ResourcePriorityProperty.TRANSIENT
        .equals(irEdge.getSrc().getPropertyValue(ResourcePriorityProperty.class).get())
        && ResourcePriorityProperty.RESERVED
        .equals(irEdge.getDst().getPropertyValue(ResourcePriorityProperty.class).get());
  }

  /**
   * checks if the edge is from reserved container to a transient container.
   * @param irEdge edge to check.
   * @return whether or not the edge satisfies the condition.
   */
  static boolean fromReservedToTransient(final IREdge irEdge) {
    return ResourcePriorityProperty.RESERVED
        .equals(irEdge.getSrc().getPropertyValue(ResourcePriorityProperty.class).get())
        && ResourcePriorityProperty.TRANSIENT
        .equals(irEdge.getDst().getPropertyValue(ResourcePriorityProperty.class).get());
  }
}
