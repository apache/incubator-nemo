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
 * Pado pass for tagging operators.
 */
public final class PadoOperatorPass implements Pass {
  public DAG process(final DAG dag) throws Exception {
    dag.doTopological(operator -> {
      final Optional<List<Edge>> inEdges = dag.getInEdgesOf(operator);
      if (!inEdges.isPresent()) {
        operator.setAttr(Attributes.Key.Placement, Attributes.Transient);
      } else {
        if (hasM2M(inEdges.get()) || allFromReserved(inEdges.get())) {
          operator.setAttr(Attributes.Key.Placement, Attributes.Reserved);
        } else {
          operator.setAttr(Attributes.Key.Placement, Attributes.Transient);
        }
      }
    });
    return dag;
  }

  private boolean hasM2M(final List<Edge> edges) {
    return edges.stream().filter(edge -> edge.getType() == Edge.Type.ScatterGather).count() > 0;
  }

  private boolean allFromReserved(final List<Edge> edges) {
    return edges.stream()
        .allMatch(edge -> edge.getSrc().getAttr(Attributes.Key.Placement) == Attributes.Reserved);
  }
}
