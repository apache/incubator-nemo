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
package edu.snu.vortex.compiler.optimizer;

import edu.snu.vortex.compiler.ir.Attributes;
import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.ir.Edge;
import edu.snu.vortex.compiler.ir.operator.Operator;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class Optimizer {
  /**
   * TODO #29: Make Optimizer Configurable
   */
  public void optimize(final DAG dag) {
    final List<Operator> topoSorted = new LinkedList<>();
    DAG.doDFS(dag, (operator -> topoSorted.add(0, operator)), DAG.VisitOrder.PostOrder);
    topoSorted.forEach(operator -> {
      final Optional<List<Edge>> inEdges = dag.getInEdges(operator);
      if (!inEdges.isPresent()) {
        operator.setAttr(Attributes.Key.Placement, Attributes.Placement.Transient);
      } else {
        if (hasM2M(inEdges.get()) || allFromReserved(inEdges.get())) {
          operator.setAttr(Attributes.Key.Placement, Attributes.Placement.Reserved);
        } else {
          operator.setAttr(Attributes.Key.Placement, Attributes.Placement.Transient);
        }
      }
    });
  }

  private boolean hasM2M(final List<Edge> edges) {
    return edges.stream().filter(edge -> edge.getType() == Edge.Type.M2M).count() > 0;
  }

  private boolean allFromReserved(final List<Edge> edges) {
    return edges.stream()
        .allMatch(edge -> edge.getSrc().getAttr(Attributes.Key.Placement) == Attributes.Placement.Reserved);
  }
}
