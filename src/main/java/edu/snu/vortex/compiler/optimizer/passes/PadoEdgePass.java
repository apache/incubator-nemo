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
package edu.snu.vortex.compiler.optimizer.passes;

import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.common.dag.DAG;

import java.util.List;

/**
 * Pado pass for tagging edges.
 */
public final class PadoEdgePass implements Pass {
  @Override
  public DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag) throws Exception {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      if (!inEdges.isEmpty()) {
        inEdges.forEach(edge -> {
          if (fromTransientToReserved(edge)) {
            edge.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.LocalFile);
            edge.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Push);
          } else if (fromReservedToTransient(edge)) {
            edge.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.LocalFile);
            edge.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Pull);
          } else {
            if (edge.getType().equals(IREdge.Type.OneToOne)) {
              edge.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
              edge.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Pull);
            } else {
              edge.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.LocalFile);
              edge.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Pull);
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
    return irEdge.getSrc().getAttr(Attribute.Key.Placement).equals(Attribute.Transient)
        && irEdge.getDst().getAttr(Attribute.Key.Placement).equals(Attribute.Reserved);
  }

  /**
   * checks if the edge is from reserved container to a transient container.
   * @param irEdge edge to check.
   * @return whether or not the edge satisfies the condition.
   */
  private static boolean fromReservedToTransient(final IREdge irEdge) {
    return irEdge.getSrc().getAttr(Attribute.Key.Placement).equals(Attribute.Reserved)
        && irEdge.getDst().getAttr(Attribute.Key.Placement).equals(Attribute.Transient);
  }
}
