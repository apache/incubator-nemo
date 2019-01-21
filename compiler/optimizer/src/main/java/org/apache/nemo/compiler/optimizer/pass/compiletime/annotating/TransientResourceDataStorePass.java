/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.List;

/**
 * Transient resource pass for tagging edges with DataStore ExecutionProperty.
 */
@Annotates(DataStoreProperty.class)
@Requires(ResourcePriorityProperty.class)
public final class TransientResourceDataStorePass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public TransientResourceDataStorePass() {
    super(TransientResourceDataStorePass.class);
  }

  @Override
  public void optimize(final IRDAG dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      if (!inEdges.isEmpty()) {
        inEdges.forEach(edge -> {
          if (fromTransientToReserved(edge) || fromReservedToTransient(edge)) {
            edge.setPropertyPermanently(DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore));
          } else if (CommunicationPatternProperty.Value.OneToOne
              .equals(edge.getPropertyValue(CommunicationPatternProperty.class).get())) {
            edge.setPropertyPermanently(DataStoreProperty.of(DataStoreProperty.Value.MemoryStore));
          } else {
            edge.setPropertyPermanently(DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore));
          }
        });
      }
    });
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
