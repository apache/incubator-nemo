/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.common.dag;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nemo.common.exception.IllegalEdgeOperationException;
import org.apache.nemo.common.ir.vertex.LoopVertex;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Methods for querying a directed-acyclic graph (DAG).
 * Implementations of the methods must not modify the topology of the DAG.
 *
 * @param <V> the vertex type
 * @param <E> the edge type
 */
public interface DAGInterface<V extends Vertex, E extends Edge<V>> extends Serializable {
  /**
   * Retrieves the vertex given its ID.
   * @param id of the vertex to retrieve
   * @return the vertex
   */
  V getVertexById(final String id);

  /**
   * Retrieves the vertices of this DAG.
   * @return the list of vertices.
   * Note that the result is never null, ensured by {@link DAGBuilder}.
   */
  List<V> getVertices();

  /**
   * Retrieves the edges of this DAG.
   * @return the list of edges.
   */
  List<E> getEdges();

  /**
   * Retrieves the root vertices of this DAG.
   * @return the list of root vertices.
   */
  List<V> getRootVertices();

  /**
   * Retrieves the incoming edges of the given vertex.
   * @param v the subject vertex.
   * @return the list of incoming edges to the vertex.
   * Note that the result is never null, ensured by {@link DAGBuilder}.
   */
  List<E> getIncomingEdgesOf(final V v);

  /**
   * Retrieves the incoming edges of the given vertex.
   * @param vertexId the ID of the subject vertex.
   * @return the list of incoming edges to the vertex.
   * Note that the result is never null, ensured by {@link DAGBuilder}.
   */
  List<E> getIncomingEdgesOf(final String vertexId);

  /**
   * Retrieves the outgoing edges of the given vertex.
   * @param v the subject vertex.
   * @return the list of outgoing edges to the vertex.
   * Note that the result is never null, ensured by {@link DAGBuilder}.
   */
  List<E> getOutgoingEdgesOf(final V v);

  /**
   * Retrieves the outgoing edges of the given vertex.
   * @param vertexId the ID of the subject vertex.
   * @return the list of outgoing edges to the vertex.
   * Note that the result is never null, ensured by {@link DAGBuilder}.
   */
  List<E> getOutgoingEdgesOf(final String vertexId);

  /**
   * Retrieves the parent vertices of the given vertex.
   * @param vertexId the ID of the subject vertex.
   * @return the list of parent vertices.
   */
  List<V> getParents(final String vertexId);

  /**
   * Retrieves the children vertices of the given vertex.
   * @param vertexId the ID of the subject vertex.
   * @return the list of children vertices.
   */
  List<V> getChildren(final String vertexId);

  /**
   * Retrieves the edge between two vertices.
   * @param srcVertexId the ID of the source vertex.
   * @param dstVertexId the ID of the destination vertex.
   * @return the edge if exists.
   * @throws IllegalEdgeOperationException otherwise.
   */
  E getEdgeBetween(final String srcVertexId, final String dstVertexId) throws IllegalEdgeOperationException;

  /**
   * Gets the DAG's vertices in topologically sorted order.
   * This function brings consistent results.
   * @return the sorted list of vertices in topological order.
   */
  List<V> getTopologicalSort();

  /**
   * Retrieves the ancestors of a vertex.
   * @param vertexId to find the ancestors for.
   * @return the list of ancestors.
   */
  List<V> getAncestors(final String vertexId);

  /**
   * Retrieves the descendants of a vertex.
   * @param vertexId to find the descendants for.
   * @return the list of descendants.
   */
  List<V> getDescendants(final String vertexId);

  /**
   * Filters the vertices according to the given condition.
   * @param condition that must be satisfied to be included in the filtered list.
   * @return the list of vertices that meet the condition.
   */
  List<V> filterVertices(final Predicate<V> condition);

  /**
   * Applies the function to each node in the DAG in a topological order.
   * This function brings consistent results.
   * @param function to apply.
   */
  void topologicalDo(final Consumer<V> function);

  /**
   * Indicates the traversal order of this DAG.
   */
  enum TraversalOrder {
    PreOrder,
    PostOrder
  }

  /**
   * Traverses the DAG by DFS, applying the given function.
   * @param function to apply.
   * @param traversalOrder which the DFS should be conducted.
   */
  void dfsTraverse(final Consumer<V> function, final TraversalOrder traversalOrder);

  /**
   * A recursive helper function for {@link #dfsTraverse(Consumer, TraversalOrder)}.
   * @param vertex the root vertex of the remaining DAG.
   * @param vertexConsumer the function to apply.
   * @param traversalOrder which the DFS should be conducted.
   * @param visited the set of nodes visited.
   */
  void dfsDo(final V vertex,
             final Consumer<V> vertexConsumer,
             final TraversalOrder traversalOrder,
             final Set<V> visited);

  /**
   * Function checks whether there is a path between two vertices.
   * @param v1 First vertex to check.
   * @param v2 Second vertex to check.
   * @return Whether or not there is a path between two vertices.
   */
  Boolean pathExistsBetween(final V v1, final V v2);

  /**
   * Checks whether the given vertex is assigned with a wrapping LoopVertex.
   * @param v Vertex to check.
   * @return whether or not it is wrapped by a LoopVertex
   */
  Boolean isCompositeVertex(final V v);

  /**
   * Retrieves the stack depth of the given vertex.
   * @param v Vertex to check.
   * @return The depth of the stack of LoopVertices for the vertex.
   */
  Integer getLoopStackDepthOf(final V v);

  /**
   * Retrieves the wrapping LoopVertex of the vertex.
   * @param v Vertex to check.
   * @return The wrapping LoopVertex.
   */
  LoopVertex getAssignedLoopVertexOf(final V v);

  /**
   * @return {@link com.fasterxml.jackson.databind.JsonNode} for this DAG.
   */
  ObjectNode asJsonNode();

  /**
   * Stores JSON representation of this DAG into a file.
   * @param directory the directory which JSON representation is saved to
   * @param name name of this DAG
   * @param description description of this DAG
   */
  void storeJSON(final String directory, final String name, final String description);
}
