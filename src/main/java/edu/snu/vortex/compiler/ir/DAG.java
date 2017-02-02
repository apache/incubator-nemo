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

import java.util.*;
import java.util.function.Consumer;

/**
 * Physical execution plan of a user program.
 */
public final class DAG {
  private final Map<String, List<Edge>> id2inEdges;
  private final Map<String, List<Edge>> id2outEdges;
  private final List<Operator> operators;

  DAG(final List<Operator> operators,
      final Map<String, List<Edge>> id2inEdges,
      final Map<String, List<Edge>> id2outEdges) {
    this.operators = operators;
    this.id2inEdges = id2inEdges;
    this.id2outEdges = id2outEdges;
  }

  public List<Operator> getOperators() {
    return operators;
  }

  Map<String, List<Edge>> getId2inEdges() {
    return id2inEdges;
  }

  Map<String, List<Edge>> getId2outEdges() {
    return id2outEdges;
  }

  /**
   * Gets the edges coming in to the given operator.
   * @param operator .
   * @return .
   */
  public Optional<List<Edge>> getInEdgesOf(final Operator operator) {
    final List<Edge> inEdges = id2inEdges.get(operator.getId());
    return inEdges == null ? Optional.empty() : Optional.of(inEdges);
  }

  /**
   * Gets the edges going out of the given operator.
   * @param operator .
   * @return .
   */
  public Optional<List<Edge>> getOutEdgesOf(final Operator operator) {
    final List<Edge> outEdges = id2outEdges.get(operator.getId());
    return outEdges == null ? Optional.empty() : Optional.of(outEdges);
  }

  /**
   * Finds the edge between two operators in the DAG.
   * @param operator1 .
   * @param operator2 .
   * @return .
   */
  public Optional<Edge> getEdgeBetween(final Operator operator1, final Operator operator2) {
    final Optional<List<Edge>> inEdges = this.getInEdgesOf(operator1);
    final Optional<List<Edge>> outEdges = this.getOutEdgesOf(operator1);
    final Set<Edge> edges = new HashSet<>();

    if (inEdges.isPresent()) {
      inEdges.get().forEach(e -> {
        if (e.getSrc().equals(operator2)) {
          edges.add(e);
        }
      });
    }
    if (outEdges.isPresent()) {
      outEdges.get().forEach(e -> {
        if (e.getDst().equals(operator2)) {
          edges.add(e);
        }
      });
    }

    if (edges.size() > 1) {
      throw new RuntimeException("There are more than one edge between two operators, this should never happen");
    } else if (edges.size() == 1) {
      return Optional.of(edges.iterator().next());
    } else {
      return Optional.empty();
    }
  }

  /////////////// Auxiliary overriding functions

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DAG dag = (DAG) o;

    if (!id2inEdges.equals(dag.id2inEdges)) {
      return false;
    }
    if (!id2outEdges.equals(dag.id2outEdges)) {
      return false;
    }
    return operators.equals(dag.operators);
  }

  @Override
  public int hashCode() {
    int result = id2inEdges.hashCode();
    result = 31 * result + id2outEdges.hashCode();
    result = 31 * result + operators.hashCode();
    return result;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    this.doDFS((operator -> {
      sb.append("<operator> ");
      sb.append(operator.toString());
      sb.append(" / <inEdges> ");
      sb.append(this.getInEdgesOf(operator).toString());
      sb.append("\n");
    }), VisitOrder.PreOrder);
    return sb.toString();
  }

  /**
   * check if the DAGBuilder contains the operator.
   * @param operator .
   * @return .
   */
  public boolean contains(final Operator operator) {
    return operators.contains(operator);
  }

  /**
   * check if the DAGBuilder contains the edge.
   * @param edge .
   * @return .
   */
  public boolean contains(final Edge edge) {
    return (id2inEdges.containsValue(edge) || id2outEdges.containsValue(edge));
  }

  /**
   * returns the number of operators in the DAGBuilder.
   * @return .
   */
  public int size() {
    return operators.size();
  }

  ////////// DFS Traversal

  /**
   * Visit order for the traversal.
   */
  public enum VisitOrder {
    PreOrder,
    PostOrder
  }

  /**
   * Do a DFS traversal.
   * @param function function to apply to each operator
   */
  public void doDFS(final Consumer<Operator> function) {
    doDFS(function, VisitOrder.PreOrder);
  }

  /**
   * Do a DFS traversal with a given visiting order.
   * @param function function to apply to each operator
   * @param visitOrder visiting order.
   */
  public void doDFS(final Consumer<Operator> function, final VisitOrder visitOrder) {
    final HashSet<Operator> visited = new HashSet<>();
    getOperators().stream()
        .filter(operator -> !id2inEdges.containsKey(operator.getId())) // root Operators
        .filter(operator -> !visited.contains(operator))
        .forEach(operator -> visitDFS(operator, function, visitOrder, visited));
  }

  private void visitDFS(final Operator operator,
                        final Consumer<Operator> operatorConsumer,
                        final VisitOrder visitOrder,
                        final HashSet<Operator> visited) {
    visited.add(operator);
    if (visitOrder == VisitOrder.PreOrder) {
      operatorConsumer.accept(operator);
    }
    final Optional<List<Edge>> outEdges = getOutEdgesOf(operator);
    if (outEdges.isPresent()) {
      outEdges.get().stream()
          .map(outEdge -> outEdge.getDst())
          .filter(outOperator -> !visited.contains(outOperator))
          .forEach(outOperator -> visitDFS(outOperator, operatorConsumer, visitOrder, visited));
    }
    if (visitOrder == VisitOrder.PostOrder) {
      operatorConsumer.accept(operator);
    }
  }
}

