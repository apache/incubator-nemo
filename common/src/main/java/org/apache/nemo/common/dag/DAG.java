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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nemo.common.exception.IllegalEdgeOperationException;
import org.apache.nemo.common.exception.IllegalVertexOperationException;
import org.apache.nemo.common.ir.vertex.LoopVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * DAG implementation.
 * @param <V> the vertex type
 * @param <E> the edge type
 */
public final class DAG<V extends Vertex, E extends Edge<V>> implements DAGInterface<V, E> {
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

  @Override
  public V getVertexById(final String id) {
    for (final V vertex : vertices) {
      if (vertex.getId().equals(id)) {
        return vertex;
      }
    }
    throw new IllegalVertexOperationException("There is no vertex of id: " + id);
  }

  @Override
  public List<V> getVertices() {
    return vertices;
  }

  @Override
  public List<E> getEdges() {
    return incomingEdges.values().stream().flatMap(List::stream).collect(Collectors.toList());
  }

  @Override
  public List<V> getRootVertices() {
    return rootVertices;
  }

  @Override
  public List<E> getIncomingEdgesOf(final V v) {
    return getIncomingEdgesOf(v.getId());
  }

  @Override
  public List<E> getIncomingEdgesOf(final String vertexId) {
    return incomingEdges.get(vertexId);
  }

  @Override
  public List<E> getOutgoingEdgesOf(final V v) {
    return getOutgoingEdgesOf(v.getId());
  }

  @Override
  public List<E> getOutgoingEdgesOf(final String vertexId) {
    return outgoingEdges.get(vertexId);
  }

  @Override
  public List<V> getParents(final String vertexId) {
    return incomingEdges.get(vertexId).stream().map(Edge::getSrc).collect(Collectors.toList());
  }

  @Override
  public List<V> getChildren(final String vertexId) {
    return outgoingEdges.get(vertexId).stream().map(Edge::getDst).collect(Collectors.toList());
  }

  @Override
  public E getEdgeBetween(final String srcVertexId, final String dstVertexId) throws IllegalEdgeOperationException {
    for (E e : incomingEdges.get(dstVertexId)) {
      if (e.getSrc().getId().equals(srcVertexId)) {
        return e;
      }
    }
    throw new IllegalEdgeOperationException(
        new Throwable("There exists no edge from " + srcVertexId + " to " + dstVertexId));
  }

  @Override
  public List<V> getTopologicalSort() {
    final List<V> sortedList = new ArrayList<>(vertices.size());
    topologicalDo(sortedList::add);
    return sortedList;
  }

  @Override
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

  @Override
  public List<V> getDescendants(final String vertexId) {
    final List<V> descendants = new ArrayList<>();
    final Set<V> visited = new HashSet<>();
    final V vertex = getVertexById(vertexId);
    dfsDo(vertex, descendants::add, TraversalOrder.PostOrder, visited);
    descendants.remove(vertex);
    return descendants;
  }

  @Override
  public List<V> filterVertices(final Predicate<V> condition) {
    final List<V> filteredVertices = vertices.stream().filter(condition).collect(Collectors.toList());
    return filteredVertices;
  }

  @Override
  public void topologicalDo(final Consumer<V> function) {
    final Stack<V> stack = new Stack<>();
    dfsTraverse(stack::push, TraversalOrder.PostOrder);
    while (!stack.isEmpty()) {
      function.accept(stack.pop());
    }
  }

  @Override
  public void dfsTraverse(final Consumer<V> function, final TraversalOrder traversalOrder) {
    final Set<V> visited = new HashSet<>();
    getVertices().stream().filter(vertex -> incomingEdges.get(vertex.getId()).isEmpty()) // root Operators
        .filter(vertex -> !visited.contains(vertex))
        .forEachOrdered(vertex -> dfsDo(vertex, function, traversalOrder, visited));
  }

  @Override
  public void dfsDo(final V vertex,
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

  @Override
  public Boolean pathExistsBetween(final V v1, final V v2) {
    final Set<V> reachableFromV1 = new HashSet<>();
    final Set<V> reachableFromV2 = new HashSet<>();

    this.dfsDo(v1, (v) -> { }, TraversalOrder.PostOrder, reachableFromV1);
    this.dfsDo(v2, (v) -> { }, TraversalOrder.PostOrder, reachableFromV2);

    return reachableFromV1.contains(v2) || reachableFromV2.contains(v1);
  }

  @Override
  public Boolean isCompositeVertex(final V v) {
    return this.assignedLoopVertexMap.containsKey(v.getId());
  }

  @Override
  public LoopVertex getAssignedLoopVertexOf(final V v) {
    return this.assignedLoopVertexMap.get(v.getId());
  }

  @Override
  public Integer getLoopStackDepthOf(final V v) {
    return this.loopStackDepthMap.get(v.getId());
  }

  @Override
  public ObjectNode asJsonNode() {
    final ObjectMapper mapper = new ObjectMapper();
    final ObjectNode node = mapper.createObjectNode();

    final ArrayNode verticesNode = mapper.createArrayNode();
    for (final V vertex : vertices) {
      final ObjectNode vertexNode = mapper.createObjectNode();
      vertexNode.put("id", vertex.getId());
      vertexNode.set("properties", vertex.getPropertiesAsJsonNode());
      verticesNode.add(vertexNode);
    }
    node.set("vertices", verticesNode);

    final ArrayNode edgesNode = mapper.createArrayNode();
    for (final List<E> edges : incomingEdges.values()) {
      for (final E edge : edges) {
        final ObjectNode edgeNode = mapper.createObjectNode();
        edgeNode.put("src", edge.getSrc().getId());
        edgeNode.put("dst", edge.getDst().getId());
        edgeNode.set("properties", edge.getPropertiesAsJsonNode());
        edgesNode.add(edgeNode);
      }

    }
    node.set("edges", edgesNode);
    return node;
  }

  @Override
  public String toString() {
    return asJsonNode().toString();
  }

  public static final String EMPTY_DAG_DIRECTORY = "";

  @Override
  public void storeJSON(final String directory, final String name, final String description) {
    if (directory == null || directory.equals(EMPTY_DAG_DIRECTORY)) {
      return;
    }

    final File file = new File(directory, name + ".json");
    file.getParentFile().mkdirs();
    try (final PrintWriter printWriter = new PrintWriter(file)) {
      printWriter.println(toString());
      printWriter.close();
      LOG.debug(String.format("DAG JSON for %s is saved at %s"
          + " (Use https://service.jangho.kr/nemo-dag/ to visualize it.)", description, file.getPath()));
    } catch (IOException e) {
      LOG.warn(String.format("Cannot store JSON representation of %s to %s: %s",
          description, file.getPath(), e.toString()));
    }
  }
}
