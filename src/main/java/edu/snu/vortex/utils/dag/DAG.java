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
package edu.snu.vortex.utils.dag;

import edu.snu.vortex.runtime.exception.IllegalVertexOperationException;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DAG implementation.
 * @param <V> the vertex type
 * @param <E> the edge type
 */
public final class DAG<V extends Vertex, E extends Edge<V>> {
  private static final Logger LOG = Logger.getLogger(DAG.class.getName());

  private final Set<V> vertices;
  private final Map<V, Set<E>> incomingEdges;
  private final Map<V, Set<E>> outgoingEdges;

  public DAG(final Set<V> vertices,
             final Map<V, Set<E>> incomingEdges,
             final Map<V, Set<E>> outgoingEdges) {
    this.vertices = vertices;
    this.incomingEdges = incomingEdges;
    this.outgoingEdges = outgoingEdges;
  }

  /**
   * Converts a DAG into another DAG according to a function.
   * @param function to apply when converting a DAG to another.
   * @param <V2> the converted DAG's vertex type.
   * @param <E2> the converted DAG's edge type.
   * @return the converted DAG.
   */
  // TODO #153: DAG conversion using Vertex/Edge Converters
  public <V2 extends Vertex, E2 extends Edge<V2>> DAG<V2, E2> convert(final Function<DAG<V, E>, DAG<V2, E2>> function) {
    return function.apply(this);
  }

  /**
   * Retrieves the vertices of this DAG.
   * @return the set of vertices.
   * Note that the result is never null, ensured by {@link DAGBuilder}.
   */
  public Set<V> getVertices() {
    return vertices;
  }

  /**
   * Retrieves the incoming edges of the given vertex.
   * @param v the subject vertex.
   * @return the set of incoming edges to the vertex.
   * Note that the result is never null, ensured by {@link DAGBuilder}.
   */
  public Set<E> getIncomingEdgesOf(final V v) {
    if (!vertices.contains(v)) {
      throw new IllegalVertexOperationException("The DAG does not contain this vertex");
    }
    return incomingEdges.get(v);
  }

  /**
   * Retrieves the outgoing edges of the given vertex.
   * @param v the subject vertex.
   * @return the set of outgoing edges to the vertex.
   * Note that the result is never null, ensured by {@link DAGBuilder}.
   */
  public Set<E> getOutgoingEdgesOf(final V v) {
    if (!vertices.contains(v)) {
      throw new IllegalVertexOperationException("The DAG does not contain this vertex");
    }
    return outgoingEdges.get(v);
  }

  /**
   * Indicates the traversal order of this DAG.
   */
  public enum TraversalOrder {
    PreOrder,
    PostOrder
  }

  /**
   * Gets the DAG's vertices in topologically sorted order.
   * @return the sorted list of vertices in topological order.
   */
  public List<V> getTopologicalSort() {
    final List<V> sortedList = new ArrayList<>(vertices.size());
    topologicalDo(v -> sortedList.add(v));
    return sortedList;
  }

  /**
   * Applies the function to each node in the DAG in a topological order.
   * @param function to apply.
   */
  public void topologicalDo(final Consumer<V> function) {
    final Stack<V> stack = new Stack<>();
    dfsTraverse(op -> stack.push(op), TraversalOrder.PostOrder);
    while (!stack.isEmpty()) {
      function.accept(stack.pop());
    }
  }

  /**
   * Traverses the DAG by DFS, applying the given function.
   * @param function to apply.
   * @param traversalOrder which the DFS should be conducted.
   */
  private void dfsTraverse(final Consumer<V> function, final TraversalOrder traversalOrder) {
    final Set<V> visited = new HashSet<>();
    getVertices().stream()
        .filter(vertex -> incomingEdges.get(vertex).isEmpty()) // root Operators
        .filter(vertex -> !visited.contains(vertex))
        .forEach(vertex -> dfsDo(vertex, function, traversalOrder, visited));
  }

  /**
   * A recursive helper function for {@link #dfsTraverse(Consumer, TraversalOrder)}.
   * @param vertex the root vertex of the remaining DAG.
   * @param vertexConsumer the function to apply.
   * @param traversalOrder which the DFS should be conducted.
   * @param visited the set of nodes visited.
   */
  private void dfsDo(final V vertex,
                     final Consumer<V> vertexConsumer,
                     final TraversalOrder traversalOrder,
                     final Set<V> visited) {
    visited.add(vertex);
    if (traversalOrder == TraversalOrder.PreOrder) {
      vertexConsumer.accept(vertex);
    }
    final Set<E> outEdges = getOutgoingEdgesOf(vertex);
    if (!outEdges.isEmpty()) {
      outEdges.stream()
          .map(outEdge -> outEdge.getDst())
          .filter(outOperator -> !visited.contains(outOperator))
          .forEach(outOperator -> dfsDo(outOperator, vertexConsumer, traversalOrder, visited));
    }
    if (traversalOrder == TraversalOrder.PostOrder) {
      vertexConsumer.accept(vertex);
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"vertices\": [");
    boolean isFirstVertex = true;
    for (final V vertex : vertices) {
      if (!isFirstVertex) {
        sb.append(", ");
      }
      isFirstVertex = false;
      sb.append("{\"id\": \"").append(vertex.getId());
      sb.append("\", \"properties\": ").append(vertex.propertiesToJSON());
      sb.append("}");
    }
    sb.append("], \"edges\": [");
    boolean isFirstEdge = true;
    for (final Set<E> edgeSet : incomingEdges.values()) {
      for (final E edge : edgeSet) {
        if (!isFirstEdge) {
          sb.append(", ");
        }
        isFirstEdge = false;
        sb.append("{\"src\": \"").append(edge.getSrc().getId());
        sb.append("\", \"dst\": \"").append(edge.getDst().getId());
        sb.append("\", \"properties\": ").append(edge.propertiesToJSON());
        sb.append("}");
      }
    }
    sb.append("]}");
    return sb.toString();
  }

  /**
   * Stores JSON representation of this DAG into a file.
   * @param directory the directory which JSON representation is saved to
   * @param name name of this DAG
   * @param description description of this DAG
   */
  public void storeJSON(final String directory, final String name, final String description) {
    final File file = new File(directory, name + ".json");
    file.getParentFile().mkdirs();
    try {
      final PrintWriter printWriter = new PrintWriter(file);
      printWriter.println(toString());
      printWriter.close();
      LOG.log(Level.INFO, String.format("DAG JSON for %s is saved at %s" +
          " (Use https://service.jangho.kr/vortex-dag/ to visualize it.)", description, file.getPath()));
    } catch (IOException e) {
      LOG.log(Level.WARNING, String.format("Cannot store JSON representation of %s to %s: %s",
          description, file.getPath(), e.toString()));
    }
  }
}
