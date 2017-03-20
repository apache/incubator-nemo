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

import java.util.*;
import java.util.function.Consumer;

/**
 * DAG representation of a user program.
 */
public final class DAG {
  private final Map<String, List<Edge>> id2inEdges;
  private final Map<String, List<Edge>> id2outEdges;
  private final List<Vertex> vertices;

  DAG(final List<Vertex> vertices,
      final Map<String, List<Edge>> id2inEdges,
      final Map<String, List<Edge>> id2outEdges) {
    this.vertices = vertices;
    this.id2inEdges = id2inEdges;
    this.id2outEdges = id2outEdges;
  }

  public List<Vertex> getVertices() {
    return vertices;
  }

  Map<String, List<Edge>> getId2inEdges() {
    return id2inEdges;
  }

  Map<String, List<Edge>> getId2outEdges() {
    return id2outEdges;
  }

  /**
   * Gets the edges coming in to the given vertex.
   * @param vertex .
   * @return .
   */
  public Optional<List<Edge>> getInEdgesOf(final Vertex vertex) {
    final List<Edge> inEdges = id2inEdges.get(vertex.getId());
    return inEdges == null ? Optional.empty() : Optional.of(inEdges);
  }

  /**
   * Gets the edges going out of the given vertex.
   * @param vertex .
   * @return .
   */
  public Optional<List<Edge>> getOutEdgesOf(final Vertex vertex) {
    final List<Edge> outEdges = id2outEdges.get(vertex.getId());
    return outEdges == null ? Optional.empty() : Optional.of(outEdges);
  }

  /**
   * Finds the edge between two vertices in the DAG.
   * @param vertex1 .
   * @param vertex2 .
   * @return .
   */
  public Optional<Edge> getEdgeBetween(final Vertex vertex1, final Vertex vertex2) {
    final Optional<List<Edge>> inEdges = this.getInEdgesOf(vertex1);
    final Optional<List<Edge>> outEdges = this.getOutEdgesOf(vertex1);
    final Set<Edge> edges = new HashSet<>();

    if (inEdges.isPresent()) {
      inEdges.get().forEach(e -> {
        if (e.getSrc().equals(vertex2)) {
          edges.add(e);
        }
      });
    }
    if (outEdges.isPresent()) {
      outEdges.get().forEach(e -> {
        if (e.getDst().equals(vertex2)) {
          edges.add(e);
        }
      });
    }

    if (edges.size() > 1) {
      throw new RuntimeException("There are more than one edge between two vertices, this should never happen");
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
    return vertices.equals(dag.vertices);
  }

  @Override
  public int hashCode() {
    int result = id2inEdges.hashCode();
    result = 31 * result + id2outEdges.hashCode();
    result = 31 * result + vertices.hashCode();
    return result;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    this.doTopological(vertex -> {
      sb.append("<vertex> ");
      sb.append(vertex.toString());
      sb.append(" / <inEdges> ");
      sb.append(this.getInEdgesOf(vertex).toString());
      sb.append("\n");
    });
    return sb.toString();
  }

  /**
   * check if the DAGBuilder contains the vertex.
   * @param vertex .
   * @return .
   */
  public boolean contains(final Vertex vertex) {
    return vertices.contains(vertex);
  }

  /**
   * check if the DAGBuilder contains the edge.
   * @param edge .
   * @return .
   */
  public boolean contains(final Edge edge) {
    return (id2inEdges.values().stream().filter(list -> list.contains(edge)).count() > 0 ||
        id2outEdges.values().stream().filter(list -> list.contains(edge)).count() > 0);
  }

  /**
   * returns the number of vertices in the DAGBuilder.
   * @return .
   */
  public int size() {
    return vertices.size();
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
   * Apply the function in a topological order.
   * @param function function to apply.
   */
  public void doTopological(final Consumer<Vertex> function) {
    final Stack<Vertex> stack = new Stack<>();
    doDFS(op -> stack.push(op), VisitOrder.PostOrder);
    while (!stack.isEmpty()) {
      function.accept(stack.pop());
    }
  }

  /**
   * Do a DFS traversal with a given visiting order.
   * @param function function to apply to each vertex
   * @param visitOrder visiting order.
   */
  private void doDFS(final Consumer<Vertex> function, final VisitOrder visitOrder) {
    final HashSet<Vertex> visited = new HashSet<>();
    getVertices().stream()
        .filter(vertex -> !id2inEdges.containsKey(vertex.getId())) // root Operators
        .filter(vertex -> !visited.contains(vertex))
        .forEach(vertex -> visitDFS(vertex, function, visitOrder, visited));
  }

  private void visitDFS(final Vertex vertex,
                        final Consumer<Vertex> vertexConsumer,
                        final VisitOrder visitOrder,
                        final HashSet<Vertex> visited) {
    visited.add(vertex);
    if (visitOrder == VisitOrder.PreOrder) {
      vertexConsumer.accept(vertex);
    }
    final Optional<List<Edge>> outEdges = getOutEdgesOf(vertex);
    if (outEdges.isPresent()) {
      outEdges.get().stream()
          .map(outEdge -> outEdge.getDst())
          .filter(outOperator -> !visited.contains(outOperator))
          .forEach(outOperator -> visitDFS(outOperator, vertexConsumer, visitOrder, visited));
    }
    if (visitOrder == VisitOrder.PostOrder) {
      vertexConsumer.accept(vertex);
    }
  }
}
