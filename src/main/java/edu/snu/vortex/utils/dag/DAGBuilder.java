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

import edu.snu.vortex.compiler.ir.LoopVertex;
import edu.snu.vortex.runtime.exception.IllegalVertexOperationException;

import java.util.*;
import java.util.function.Predicate;

/**
 * DAG Builder.
 * @param <V> the vertex type.
 * @param <E> the edge type.
 */
public final class DAGBuilder<V extends Vertex, E extends Edge<V>> {
  private final Set<V> vertices;
  private final Map<V, Set<E>> incomingEdges;
  private final Map<V, Set<E>> outgoingEdges;
  private final Map<V, LoopVertex> assignedLoopVertexMap;
  private final Map<V, Integer> loopStackDepthMap;

  /**
   * Constructor of DAGBuilder: it initializes everything.
   */
  public DAGBuilder() {
    this.vertices = new HashSet<>();
    this.incomingEdges = new HashMap<>();
    this.outgoingEdges = new HashMap<>();
    this.assignedLoopVertexMap = new HashMap<>();
    this.loopStackDepthMap = new HashMap<>();
  }

  /**
   * Add vertex to the builder.
   * @param v vertex to add.
   * @return the builder.
   */
  public DAGBuilder<V, E> addVertex(final V v) {
    vertices.add(v);
    incomingEdges.putIfAbsent(v, new HashSet<>());
    outgoingEdges.putIfAbsent(v, new HashSet<>());
    return this;
  }
  /**
   * Add vertex to the builder, with assignedLoopVertex and stackDepth information.
   * @param v vertex to add.
   * @param assignedLoopVertex the assigned, wrapping loop vertex.
   * @param stackDepth the stack depth of the loop vertex.
   * @return the builder.
   */
  private DAGBuilder<V, E> addVertex(final V v, final LoopVertex assignedLoopVertex, final Integer stackDepth) {
    addVertex(v);
    this.assignedLoopVertexMap.put(v, assignedLoopVertex);
    this.loopStackDepthMap.put(v, stackDepth);
    return this;
  }
  /**
   * Add vertex to the builder, using the LoopVertex stack.
   * @param v vertex to add.
   * @param loopVertexStack LoopVertex stack to retrieve the information from.
   * @return the builder.
   */
  public DAGBuilder<V, E> addVertex(final V v, final Stack<LoopVertex> loopVertexStack) {
    if (!loopVertexStack.empty()) {
      addVertex(v, loopVertexStack.peek(), loopVertexStack.size());
    } else {
      addVertex(v);
    }
    return this;
  }
  /**
   * Add vertex to the builder, using the information from the given DAG.
   * @param v vertex to add.
   * @param dag DAG to observe and get the LoopVertex-related information from.
   * @return the builder.
   */
  public DAGBuilder<V, E> addVertex(final V v, final DAG<V, E> dag) {
    if (dag.isCompositeVertex(v)) {
      addVertex(v, dag.getAssignedLoopVertexOf(v), dag.getLoopStackDepthOf(v));
    } else {
      addVertex(v);
    }
    return this;
  }

  /**
   * Remove the vertex from the list.
   * @param v vertex to remove.
   * @return the builder.
   */
  public DAGBuilder<V, E> removeVertex(final V v) {
    vertices.remove(v);
    incomingEdges.remove(v);
    outgoingEdges.remove(v);
    return this;
  }

  /**
   * Connect vertices at the edge.
   * @param edge edge to add.
   * Note: the two vertices of the edge should already be added to the DAGBuilder.
   * @return the builder.
   */
  public DAGBuilder<V, E> connectVertices(final E edge) {
    final V src = edge.getSrc();
    final V dst = edge.getDst();
    if (vertices.contains(src) && vertices.contains(dst)) {
      incomingEdges.get(dst).add(edge);
      outgoingEdges.get(src).add(edge);
    } else {
      throw new IllegalVertexOperationException("The DAG does not contain either src or dst of the edge: "
          + src + " -> " + dst);
    }
    return this;
  }

  /**
   * Checks whether the DAGBuilder is empty.
   * @return whether the DAGBuilder is empty or not.
   */
  public boolean isEmpty() {
    return vertices.isEmpty();
  }

  /**
   * check if the DAGBuilder contains the vertex.
   * @param vertex vertex that it searches for.
   * @return whether or not the builder contains it.
   */
  public boolean contains(final V vertex) {
    return vertices.contains(vertex);
  }

  /**
   * check if the DAGBuilder contains any vertex that satisfies the predicate.
   * @param predicate predicate to test each vertices with.
   * @return whether or not the builder contains it.
   */
  public boolean contains(final Predicate<V> predicate) {
    return vertices.stream().anyMatch(predicate);
  }

  /**
   * Build the DAG.
   * @return the DAG contained by the builder.
   */
  public DAG<V, E> build() {
    return new DAG<>(vertices, incomingEdges, outgoingEdges, assignedLoopVertexMap, loopStackDepthMap);
  }
}
