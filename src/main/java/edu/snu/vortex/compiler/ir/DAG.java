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
package edu.snu.vortex.compiler.ir;

import edu.snu.vortex.compiler.ir.operator.Operator;
import edu.snu.vortex.compiler.ir.operator.Source;

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

  public Optional<List<Edge>> getInEdges(final Operator operator) {
    final List<Edge> inEdges = id2inEdges.get(operator.getId());
    return inEdges == null ? Optional.empty() : Optional.of(inEdges);
  }

  public Optional<List<Edge>> getOutEdges(final Operator operator) {
    final List<Edge> outEdges = id2outEdges.get(operator.getId());
    return outEdges == null ? Optional.empty() : Optional.of(outEdges);
  }

  ////////// Auxiliary functions for graph construction and view

  public static void print(final DAG dag) {
    doDFS(dag, (operator -> System.out.println("<operator> " + operator + " / <inEdges> " + dag.getInEdges(operator))), VisitOrder.PreOrder);
  }

  ////////// DFS Traversal
  public enum VisitOrder {
    PreOrder,
    PostOrder
  }

  public static void doDFS(final DAG dag,
                           final Consumer<Operator> function,
                           final VisitOrder visitOrder) {
    final HashSet<Operator> visited = new HashSet<>();
    dag.getSources().stream()
        .filter(source -> !visited.contains(source))
        .forEach(source -> visit(source, function, visitOrder, dag, visited));
  }

  private static void visit(final Operator operator,
                            final Consumer<Operator> operatorConsumer,
                            final VisitOrder visitOrder,
                            final DAG dag,
                            final HashSet<Operator> visited) {
    visited.add(operator);
    if (visitOrder == VisitOrder.PreOrder) {
      operatorConsumer.accept(operator);
    }
    final Optional<List<Edge>> outEdges = dag.getOutEdges(operator);
    if (outEdges.isPresent()) {
      outEdges.get().stream()
          .map(outEdge -> outEdge.getDst())
          .filter(outOperator -> !visited.contains(outOperator))
          .forEach(outOperator -> visit(outOperator, operatorConsumer, visitOrder, dag, visited));
    }
    if (visitOrder == VisitOrder.PostOrder) {
      operatorConsumer.accept(operator);
    }
  }
}

