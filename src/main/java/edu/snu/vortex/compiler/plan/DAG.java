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
package edu.snu.vortex.compiler.plan;

import edu.snu.vortex.compiler.plan.node.Node;
import edu.snu.vortex.compiler.plan.node.Source;

import java.util.*;
import java.util.function.Consumer;

/**
 * Physical execution plan of a user program.
 */
public class DAG {
  private final Map<String, List<Edge>> id2inEdges;
  private final Map<String, List<Edge>> id2outEdges;
  private final List<Source> sources;

  public DAG(final List<Source> sources,
             final Map<String, List<Edge>> id2inEdges,
             final Map<String, List<Edge>> id2outEdges) {
    this.sources = sources;
    this.id2inEdges = id2inEdges;
    this.id2outEdges = id2outEdges;
  }

  public List<Source> getSources() {
    return sources;
  }

  public Optional<List<Edge>> getInEdges(final Node node) {
    final List<Edge> inEdges = id2inEdges.get(node.getId());
    return inEdges == null ? Optional.empty() : Optional.of(inEdges);
  }

  public Optional<List<Edge>> getOutEdges(final Node node) {
    final List<Edge> outEdges = id2outEdges.get(node.getId());
    return outEdges == null ? Optional.empty() : Optional.of(outEdges);
  }

  ////////// Auxiliary functions for graph construction and view

  public static void print(final DAG dag) {
    doDFS(dag, (node -> System.out.println("<node> " + node + " / <inEdges> " + dag.getInEdges(node))), VisitOrder.PreOrder);
  }

  ////////// DFS Traversal
  public enum VisitOrder {
    PreOrder,
    PostOrder
  }

  public static void doDFS(final DAG dag,
                           final Consumer<Node> function,
                           final VisitOrder visitOrder) {
    final HashSet<Node> visited = new HashSet<>();
    dag.getSources().stream()
        .filter(source -> !visited.contains(source))
        .forEach(source -> visit(source, function, visitOrder, dag, visited));
  }

  private static void visit(final Node node,
                            final Consumer<Node> nodeConsumer,
                            final VisitOrder visitOrder,
                            final DAG dag,
                            final HashSet<Node> visited) {
    visited.add(node);
    if (visitOrder == VisitOrder.PreOrder) {
      nodeConsumer.accept(node);
    }
    final Optional<List<Edge>> outEdges = dag.getOutEdges(node);
    if (outEdges.isPresent()) {
      outEdges.get().stream()
          .map(outEdge -> outEdge.getDst())
          .filter(outNode -> !visited.contains(outNode))
          .forEach(outNode -> visit(outNode, nodeConsumer, visitOrder, dag, visited));
    }
    if (visitOrder == VisitOrder.PostOrder) {
      nodeConsumer.accept(node);
    }
  }
}

