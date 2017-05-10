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
import edu.snu.vortex.utils.dag.DAG;

import java.util.List;

/**
 * Pado pass for tagging vertices.
 */
public final class PadoVertexPass implements Pass {
  public DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag) throws Exception {
    dag.topologicalDo(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      if (inEdges.isEmpty()) {
        vertex.setAttr(Attribute.Key.Placement, Attribute.Transient);
      } else {
        if (hasM2M(inEdges) || allFromReserved(inEdges)) {
          vertex.setAttr(Attribute.Key.Placement, Attribute.Reserved);
        } else {
          vertex.setAttr(Attribute.Key.Placement, Attribute.Transient);
        }
      }
    });
    return dag;
  }

  private boolean hasM2M(final List<IREdge> irEdges) {
    return irEdges.stream().filter(edge -> edge.getType() == IREdge.Type.ScatterGather).count() > 0;
  }

  private boolean allFromReserved(final List<IREdge> irEdges) {
    return irEdges.stream()
        .allMatch(edge -> edge.getSrc().getAttr(Attribute.Key.Placement) == Attribute.Reserved);
  }
}
