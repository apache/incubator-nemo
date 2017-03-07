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

import java.util.*;

/**
 * Optimizer class.
 */
public final class Optimizer {
  /**
   * Optimize function.
   * Currently this optimizer just performs the Pado Placement Algorithm
   * TODO #29: Make Optimizer Configurable
   * @param dag .
   * @return optimized DAG
   */
  public DAG optimize(final DAG dag) {
    operatorPlacement(dag);
    edgeProcessing(dag);
    return dag;
  }

  /////////////////////////////////////////////////////////////

  private DAG operatorPlacement(final DAG dag) {
    dag.doTopological(operator -> {
      final Optional<List<Edge>> inEdges = dag.getInEdgesOf(operator);
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
    return dag;
  }

  private boolean hasM2M(final List<Edge> edges) {
    return edges.stream().filter(edge -> edge.getType() == Edge.Type.ScatterGather).count() > 0;
  }

  private boolean allFromReserved(final List<Edge> edges) {
    return edges.stream()
        .allMatch(edge -> edge.getSrc().getAttrByKey(Attributes.Key.Placement) == Attributes.Placement.Reserved);
  }

  ///////////////////////////////////////////////////////////

  private DAG edgeProcessing(final DAG dag) {
    dag.getOperators().forEach(operator -> {
      final Optional<List<Edge>> inEdges = dag.getInEdgesOf(operator);
      if (inEdges.isPresent()) {
        inEdges.get().forEach(edge -> {
          if (fromTransientToReserved(edge)) {
            edge.setAttr(Attributes.Key.EdgeChannel, Attributes.EdgeChannel.TCPPipe);
          } else if (fromReservedToTransient(edge)) {
            edge.setAttr(Attributes.Key.EdgeChannel, Attributes.EdgeChannel.File);
          } else {
            if (edge.getType().equals(Edge.Type.OneToOne)) {
              edge.setAttr(Attributes.Key.EdgeChannel, Attributes.EdgeChannel.Memory);
            } else {
              edge.setAttr(Attributes.Key.EdgeChannel, Attributes.EdgeChannel.File);
            }
          }
        });
      }
    });
    return dag;
  }

  private boolean fromTransientToReserved(final Edge edge) {
    return edge.getSrc().getAttrByKey(Attributes.Key.Placement).equals(Attributes.Placement.Transient) &&
        edge.getDst().getAttrByKey(Attributes.Key.Placement).equals(Attributes.Placement.Reserved);
  }

  private boolean fromReservedToTransient(final Edge edge) {
    return edge.getSrc().getAttrByKey(Attributes.Key.Placement).equals(Attributes.Placement.Reserved) &&
        edge.getDst().getAttrByKey(Attributes.Key.Placement).equals(Attributes.Placement.Transient);
  }
}
