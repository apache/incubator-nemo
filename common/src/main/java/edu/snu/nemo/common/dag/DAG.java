/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.nemo.common.dag;

import edu.snu.nemo.common.exception.IllegalEdgeOperationException;
import edu.snu.nemo.common.exception.IllegalVertexOperationException;
import edu.snu.nemo.common.ir.vertex.LoopVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * DAG implementation.
 * @param <V> the vertex type
 * @param <E> the edge type
 */
public final class DAG<V extends Vertex, E extends Edge<V>> implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(DAG.class.getName());

  private final List<V> vertices;
  private final List<V> rootVertices;
  private final Map<String, List<E>> incomingEdges;
  private final Map<String, List<E>> outgoingEdges;
  private final Map<String, LoopVertex> assignedLoopVertexMap;
  private final Map<String, Integer> loopStackDepthMap;

  /**
   * Constructor of DAG, called by the DAGBuilder.
   * @param vertices set of vertices.
   * @param incomingEdges map of incoming edges for each vertex.
   * @param outgoingEdges map of outgoing edges for each vertex.
   * @param assignedLoopVertexMap map of assignedLoopVertex info.
   * @param loopStackDepthMap map of stack depth of LoopVertices.
   */
  public DAG(final Set<V> vertices,
             final Map<V, Set<E>> incomingEdges,
             final Map<V, Set<E>> outgoingEdges,
             final Map<V, LoopVertex> assignedLoopVertexMap,
             final Map<V, Integer> loopStackDepthMap) {
    this.vertices = new ArrayList<>();
    this.incomingEdges = new HashMap<>();
    this.outgoingEdges = new HashMap<>();
    vertices.stream().sorted(Comparator.comparingInt(Vertex::getNumericId)).forEachOrdered(this.vertices::add);
    incomingEdges.forEach((v, es) -> this.incomingEdges.put(v.getId(),
        es.stream().sorted(Comparator.comparingInt(Edge::getNumericId)).collect(Collectors.toList())));
    outgoingEdges.forEach((v, es) -> this.outgoingEdges.put(v.getId(),
        es.stream().sorted(Comparator.comparingInt(Edge::getNumericId)).collect(Collectors.toList())));

    this.rootVertices = new ArrayList<>();
    vertices.forEach(v -> {
      // this list is empty if there is no incoming edge, and is therefore a root vertex.
      final List<E> incomingEdgesForThisVertex = this.incomingEdges.get(v.getId());
      if (incomingEdgesForThisVertex.isEmpty()) {
        this.rootVertices.add(v);
      }
    });

    this.assignedLoopVertexMap = new HashMap<>();
    this.loopStackDepthMap = new HashMap<>();
    assignedLoopVertexMap.forEach((v, loopVertex) -> this.assignedLoopVertexMap.put(v.getId(), loopVertex));
    loopStackDepthMap.forEach(((v, integer) -> this.loopStackDepthMap.put(v.getId(), integer)));
  }

  /**
   * Converts a DAG into another DAG according to a function.
   * @param function to apply when converting a DAG to another.
   * @param <V2> the converted DAG's vertex type.
   * @param <E2> the converted DAG's edge type.
   * @return the converted DAG.
   */
  public <V2 extends Vertex, E2 extends Edge<V2>> DAG<V2, E2> convert(final Function<DAG<V, E>, DAG<V2, E2>> function) {
    return function.apply(this);
  }

  /**
   * Retrieves the vertex given its ID.
   * @param id of the vertex to retrieve
   * @return the vertex
   * @throws IllegalVertexOperationException when the requested vertex does not exist.
   */
  public V getVertexById(final String id) {
    for (final V vertex : vertices) {
      if (vertex.getId().equals(id)) {
        return vertex;
      }
    }
    throw new IllegalVertexOperationException("There is no vertex of id: " + id);
  }

  /**
   * Retrieves the vertices of this DAG.
   * @return the set of vertices.
   * Note that the result is never null, ensured by {@link DAGBuilder}.
   */
  public List<V> getVertices() {
    return vertices;
  }

  /**
   * Retrieves the root vertices of this DAG.
   * @return the set of root vertices.
   */
  public List<V> getRootVertices() {
    return rootVertices;
  }

  /**
   * Retrieves the incoming edges of the given vertex.
   * @param v the subject vertex.
   * @return the set of incoming edges to the vertex.
   * Note that the result is never null, ensured by {@link DAGBuilder}.
   */
  public List<E> getIncomingEdgesOf(final V v) {
    return getIncomingEdgesOf(v.getId());
  }

  /**
   * Retrieves the incoming edges of the given vertex.
   * @param vertexId the ID of the subject vertex.
   * @return the set of incoming edges to the vertex.
   * Note that the result is never null, ensured by {@link DAGBuilder}.
   */
  public List<E> getIncomingEdgesOf(final String vertexId) {
    return incomingEdges.get(vertexId);
  }

  /**
   * Retrieves the outgoing edges of the given vertex.
   * @param v the subject vertex.
   * @return the set of outgoing edges to the vertex.
   * Note that the result is never null, ensured by {@link DAGBuilder}.
   */
  public List<E> getOutgoingEdgesOf(final V v) {
    return getOutgoingEdgesOf(v.getId());
  }

  /**
   * Retrieves the outgoing edges of the given vertex.
   * @param vertexId the ID of the subject vertex.
   * @return the set of outgoing edges to the vertex.
   * Note that the result is never null, ensured by {@link DAGBuilder}.
   */
  public List<E> getOutgoingEdgesOf(final String vertexId) {
    return outgoingEdges.get(vertexId);
  }

  /**
   * Retrieves the parent vertices of the given vertex.
   * @param vertexId the ID of the subject vertex.
   * @return the list of parent vertices.
   */
  public List<V> getParents(final String vertexId) {
    return incomingEdges.get(vertexId).stream().map(Edge::getSrc).collect(Collectors.toList());
  }

  /**
   * Retrieves the children vertices of the given vertex.
   * @param vertexId the ID of the subject vertex.
   * @return the list of children vertices.
   */
  public List<V> getChildren(final String vertexId) {
    return outgoingEdges.get(vertexId).stream().map(Edge::getDst).collect(Collectors.toList());
  }

  /**
   * Retrieves the edge between two vertices.
   * @param srcVertexId the ID of the source vertex.
   * @param dstVertexId the ID of the destination vertex.
   * @return the edge if exists.
   * @throws IllegalEdgeOperationException otherwise.
   */
  public E getEdgeBetween(final String srcVertexId, final String dstVertexId) throws IllegalEdgeOperationException {
    for (E e : incomingEdges.get(dstVertexId)) {
      if (e.getSrc().getId().equals(srcVertexId)) {
        return e;
      }
    }
    throw new IllegalEdgeOperationException(
        new Throwable("There exists no edge from " + srcVertexId + " to " + dstVertexId));
  }

  /**
   * Indicates the traversal order of this DAG.
   */
  private enum TraversalOrder {
    PreOrder,
    PostOrder
  }

  /**
   * Gets the DAG's vertices in topologically sorted order.
   * This function brings consistent results.
   * @return the sorted list of vertices in topological order.
   */
  public List<V> getTopologicalSort() {
    final List<V> sortedList = new ArrayList<>(vertices.size());
    topologicalDo(sortedList::add);
    return sortedList;
  }

  /**
   * Retrieves the ancestors of a vertex.
   * @param vertexId to find the ancestors for.
   * @return the list of ancestors.
   */
  public List<V> getAncestors(final String vertexId) {
    final List<V> ancestors = new ArrayList<>();
    addAncestors(ancestors, vertexId);
    return ancestors;
  }

  /**
   * Recursively adds ancestors of a vertex to the given list.
   * @param ancestorList to accumulate the ancestors.
   * @param vertexId to find the ancestors for.
   */
  private void addAncestors(final List<V> ancestorList, final String vertexId) {
    getParents(vertexId).forEach(parent -> {
      ancestorList.add(parent);
      addAncestors(ancestorList, parent.getId());
    });
  }

  /**
   * Retrieves the descendants of a vertex.
   * @param vertexId to find the descendants for.
   * @return the list of descendants.
   */
  public List<V> getDescendants(final String vertexId) {
    final List<V> descendants = new ArrayList<>();
    final Set<V> visited = new HashSet<>();
    final V vertex = getVertexById(vertexId);
    dfsDo(vertex, descendants::add, TraversalOrder.PostOrder, visited);
    descendants.remove(vertex);
    return descendants;
  }

  /**
   * Filters the vertices according to the given condition.
   * @param condition that must be satisfied to be included in the filtered list.
   * @return the list of vertices that meet the condition.
   */
  public List<V> filterVertices(final Predicate<V> condition) {
    final List<V> filteredVertices = vertices.stream().filter(condition).collect(Collectors.toList());
    return filteredVertices;
  }

  /**
   * Applies the function to each node in the DAG in a topological order.
   * This function brings consistent results.
   * @param function to apply.
   */
  public void topologicalDo(final Consumer<V> function) {
    final Stack<V> stack = new Stack<>();
    dfsTraverse(stack::push, TraversalOrder.PostOrder);
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
    getVertices().stream().filter(vertex -> incomingEdges.get(vertex.getId()).isEmpty()) // root Operators
        .filter(vertex -> !visited.contains(vertex))
        .forEachOrdered(vertex -> dfsDo(vertex, function, traversalOrder, visited));
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
    final List<E> outEdges = getOutgoingEdgesOf(vertex);
    if (!outEdges.isEmpty()) {
      outEdges.stream().map(Edge::getDst)
          .filter(outOperator -> !visited.contains(outOperator))
          .forEachOrdered(outOperator -> dfsDo(outOperator, vertexConsumer, traversalOrder, visited));
    }
    if (traversalOrder == TraversalOrder.PostOrder) {
      vertexConsumer.accept(vertex);
    }
  }

  /**
   * Function checks whether there is a path between two vertices.
   * @param v1 First vertex to check.
   * @param v2 Second vertex to check.
   * @return Whether or not there is a path between two vertices.
   */
  public Boolean pathExistsBetween(final V v1, final V v2) {
    final Set<V> reachableFromV1 = new HashSet<>();
    final Set<V> reachableFromV2 = new HashSet<>();

    this.dfsDo(v1, (v) -> { }, TraversalOrder.PostOrder, reachableFromV1);
    this.dfsDo(v2, (v) -> { }, TraversalOrder.PostOrder, reachableFromV2);

    return reachableFromV1.contains(v2) || reachableFromV2.contains(v1);
  }

  /**
   * Checks whether the given vertex is assigned with a wrapping LoopVertex.
   * @param v Vertex to check.
   * @return whether or not it is wrapped by a LoopVertex
   */
  public Boolean isCompositeVertex(final V v) {
    return this.assignedLoopVertexMap.containsKey(v.getId());
  }

  /**
   * Retrieves the wrapping LoopVertex of the vertex.
   * @param v Vertex to check.
   * @return The wrapping LoopVertex.
   */
  public LoopVertex getAssignedLoopVertexOf(final V v) {
    return this.assignedLoopVertexMap.get(v.getId());
  }

  /**
   * Retrieves the stack depth of the given vertex.
   * @param v Vertex to check.
   * @return The depth of the stack of LoopVertices for the vertex.
   */
  public Integer getLoopStackDepthOf(final V v) {
    return this.loopStackDepthMap.get(v.getId());
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
    for (final List<E> edgeList : incomingEdges.values()) {
      for (final E edge : edgeList) {
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

  public static final String EMPTY_DAG_DIRECTORY = "";

  /**
   * Stores JSON representation of this DAG into a file.
   * @param directory the directory which JSON representation is saved to
   * @param name name of this DAG
   * @param description description of this DAG
   */
  public void storeJSON(final String directory, final String name, final String description) {
    if (directory == null || directory.equals(EMPTY_DAG_DIRECTORY)) {
      return;
    }

    final File file = new File(directory, name + ".json");
    file.getParentFile().mkdirs();
    try (final PrintWriter printWriter = new PrintWriter(file)) {
      printWriter.println(toString());
      printWriter.close();
      LOG.info(String.format("DAG JSON for %s is saved at %s"
          + " (Use https://service.jangho.kr/nemo-dag/ to visualize it.)", description, file.getPath()));
    } catch (IOException e) {
      LOG.warn(String.format("Cannot store JSON representation of %s to %s: %s",
          description, file.getPath(), e.toString()));
    }
  }
}
