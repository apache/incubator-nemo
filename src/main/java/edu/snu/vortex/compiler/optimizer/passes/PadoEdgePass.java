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

import edu.snu.vortex.compiler.ir.Attributes;
import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.ir.Edge;

import java.util.List;
import java.util.Optional;

/**
 * Pado pass for tagging edges.
 */
public final class PadoEdgePass implements Pass {
  public DAG process(final DAG dag) throws Exception {
    dag.getOperators().forEach(operator -> {
      final Optional<List<Edge>> inEdges = dag.getInEdgesOf(operator);
      if (inEdges.isPresent()) {
        inEdges.get().forEach(edge -> {
          if (fromTransientToReserved(edge)) {
            edge.setAttr(Attributes.Key.EdgeChannel, Attributes.TCPPipe);
          } else if (fromReservedToTransient(edge)) {
            edge.setAttr(Attributes.Key.EdgeChannel, Attributes.File);
          } else {
            if (edge.getType().equals(Edge.Type.OneToOne)) {
              edge.setAttr(Attributes.Key.EdgeChannel, Attributes.Memory);
            } else {
              edge.setAttr(Attributes.Key.EdgeChannel, Attributes.File);
            }
          }
        });
      }
    });
    return dag;
  }

  private boolean fromTransientToReserved(final Edge edge) {
    return edge.getSrc().getAttr(Attributes.Key.Placement).equals(Attributes.Transient) &&
        edge.getDst().getAttr(Attributes.Key.Placement).equals(Attributes.Reserved);
  }

  private boolean fromReservedToTransient(final Edge edge) {
    return edge.getSrc().getAttr(Attributes.Key.Placement).equals(Attributes.Reserved) &&
        edge.getDst().getAttr(Attributes.Key.Placement).equals(Attributes.Transient);
  }
}
