/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.vortex.translator.beam;

import edu.snu.vortex.compiler.plan.Attributes;
import edu.snu.vortex.compiler.plan.DAG;
import edu.snu.vortex.compiler.plan.DAGBuilder;
import edu.snu.vortex.compiler.plan.Edge;
import edu.snu.vortex.compiler.plan.node.Node;
import edu.snu.vortex.runtime.SimpleEngine;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PipelineRunner;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class Runner extends PipelineRunner<Result> {

  public static PipelineRunner<Result> fromOptions(PipelineOptions options) {
    return new Runner();
  }

  public Result run(final Pipeline pipeline) {
    final DAGBuilder builder = new DAGBuilder();
    final Visitor visitor = new Visitor(builder);
    pipeline.traverseTopologically(visitor);
    final DAG dag = builder.build();

    System.out.println("##### VORTEX COMPILER (Before Placement) #####");
    DAG.print(dag);

    System.out.println("##### VORTEX COMPILER (After Placement) #####");
    place(dag);
    DAG.print(dag);

    System.out.println("##### VORTEX ENGINE #####");
    try {
      // TODO #25: Compiler Interfaces
      SimpleEngine.executeDAG(dag);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return new Result();
  }

  // TODO #25: Compiler Interfaces
  private void place(final DAG dag) {
    final List<Node> topoSorted = new LinkedList<>();
    DAG.doDFS(dag, (node -> topoSorted.add(0, node)), DAG.VisitOrder.PostOrder);
    topoSorted.forEach(node -> {
      final Optional<List<Edge>> inEdges = dag.getInEdges(node);
      if (!inEdges.isPresent()) {
        node.setAttr(Attributes.Key.Placement, Attributes.Placement.Transient);
      } else {
        if (hasM2M(inEdges.get()) || allFromReserved(inEdges.get())) {
          node.setAttr(Attributes.Key.Placement, Attributes.Placement.Reserved);
        } else {
          node.setAttr(Attributes.Key.Placement, Attributes.Placement.Transient);
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
