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
package org.apache.nemo.common.ir.vertex;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupPropertyValue;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.IntPredicate;

/**
 * IRVertex that contains a partial DAG that is iterative.
 */
public final class LoopVertex extends IRVertex {

  private static int duplicateEdgeGroupId = 0;
  // Contains DAG information
  private final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
  private final String compositeTransformFullName;
  // for the initial iteration
  private final Map<IRVertex, Set<IREdge>> dagIncomingEdges = new HashMap<>();
  // Edges from previous iterations connected internal.
  private final Map<IRVertex, Set<IREdge>> iterativeIncomingEdges = new HashMap<>();
  // Edges from outside previous iterations.
  private final Map<IRVertex, Set<IREdge>> nonIterativeIncomingEdges = new HashMap<>();
  // for the final iteration
  private final Map<IRVertex, Set<IREdge>> dagOutgoingEdges = new HashMap<>();
  private final Map<IREdge, IREdge> edgeWithLoopToEdgeWithInternalVertex = new HashMap<>();
  private final Map<IREdge, IREdge> edgeWithInternalVertexToEdgeWithLoop = new HashMap<>();
  private Integer maxNumberOfIterations;
  private transient IntPredicate terminationCondition;

  /**
   * The LoopVertex constructor.
   *
   * @param compositeTransformFullName full name of the composite transform.
   */
  public LoopVertex(final String compositeTransformFullName) {
    super();
    this.compositeTransformFullName = compositeTransformFullName;
    this.maxNumberOfIterations = 1; // 1 is the default number of iterations.
    this.terminationCondition = (IntPredicate & Serializable) (integer -> false); // nothing much yet.
  }

  /**
   * Copy Constructor for LoopVertex.
   *
   * @param that the source object for copying
   */
  private LoopVertex(final LoopVertex that) {
    super(that);
    this.compositeTransformFullName = new String(that.compositeTransformFullName);
    // Copy all elements to the clone
    final DAG<IRVertex, IREdge> dagToCopy = that.getDAG();
    dagToCopy.topologicalDo(v -> {
      this.getBuilder().addVertex(v, dagToCopy);
      dagToCopy.getIncomingEdgesOf(v).forEach(this.getBuilder()::connectVertices);
    });
    that.dagIncomingEdges.forEach(((v, es) -> es.forEach(this::addDagIncomingEdge)));
    that.iterativeIncomingEdges.forEach((v, es) -> es.forEach(this::addIterativeIncomingEdge));
    that.nonIterativeIncomingEdges.forEach((v, es) -> es.forEach(this::addNonIterativeIncomingEdge));
    that.dagOutgoingEdges.forEach(((v, es) -> es.forEach(this::addDagOutgoingEdge)));
    that.edgeWithLoopToEdgeWithInternalVertex.forEach(this::mapEdgeWithLoop);
    this.maxNumberOfIterations = that.maxNumberOfIterations;
    this.terminationCondition = that.terminationCondition;
  }

  @Override
  public LoopVertex getClone() {
    return new LoopVertex(this);
  }

  /**
   * @return DAGBuilder of the LoopVertex.
   */
  public DAGBuilder<IRVertex, IREdge> getBuilder() {
    return builder;
  }

  /**
   * @return the DAG of the LoopVertex
   */
  public DAG<IRVertex, IREdge> getDAG() {
    return builder.buildWithoutSourceSinkCheck();
  }

  /**
   * @return the full name of the composite transform.
   */
  public String getName() {
    return compositeTransformFullName;
  }

  /**
   * Maps an edge from/to loop with the corresponding edge from/to internal vertex.
   *
   * @param edgeWithLoop           an edge from/to loop
   * @param edgeWithInternalVertex the corresponding edge from/to internal vertex
   */
  public void mapEdgeWithLoop(final IREdge edgeWithLoop, final IREdge edgeWithInternalVertex) {
    this.edgeWithLoopToEdgeWithInternalVertex.put(edgeWithLoop, edgeWithInternalVertex);
    this.edgeWithInternalVertexToEdgeWithLoop.put(edgeWithInternalVertex, edgeWithLoop);
  }

  /**
   * @param edgeWithInternalVertex an edge with internal vertex
   * @return the corresponding edge with loop for the specified edge with internal vertex
   */
  public IREdge getEdgeWithLoop(final IREdge edgeWithInternalVertex) {
    return this.edgeWithInternalVertexToEdgeWithLoop.get(edgeWithInternalVertex);
  }

  /**
   * Adds the incoming edge of the contained DAG.
   *
   * @param edge edge to add.
   */
  public void addDagIncomingEdge(final IREdge edge) {
    this.dagIncomingEdges.putIfAbsent(edge.getDst(), new HashSet<>());
    this.dagIncomingEdges.get(edge.getDst()).add(edge);
  }

  /**
   * @return incoming edges of the contained DAG.
   */
  public Map<IRVertex, Set<IREdge>> getDagIncomingEdges() {
    return this.dagIncomingEdges;
  }

  /**
   * Adds an iterative incoming edge, from the previous iteration, but connection internally.
   *
   * @param edge edge to add.
   */
  public void addIterativeIncomingEdge(final IREdge edge) {
    this.iterativeIncomingEdges.putIfAbsent(edge.getDst(), new HashSet<>());
    this.iterativeIncomingEdges.get(edge.getDst()).add(edge);
  }

  /**
   * @return the iterative incoming edges inside the DAG.
   */
  public Map<IRVertex, Set<IREdge>> getIterativeIncomingEdges() {
    return this.iterativeIncomingEdges;
  }

  /**
   * Adds a non-iterative incoming edge, from outside the previous iteration.
   *
   * @param edge edge to add.
   */
  public void addNonIterativeIncomingEdge(final IREdge edge) {
    this.nonIterativeIncomingEdges.putIfAbsent(edge.getDst(), new HashSet<>());
    this.nonIterativeIncomingEdges.get(edge.getDst()).add(edge);
  }

  /**
   * @return the non-iterative incoming edges of the LoopVertex.
   */
  public Map<IRVertex, Set<IREdge>> getNonIterativeIncomingEdges() {
    return this.nonIterativeIncomingEdges;
  }

  /**
   * Adds and outgoing edge of the contained DAG.
   *
   * @param edge edge to add.
   */
  public void addDagOutgoingEdge(final IREdge edge) {
    this.dagOutgoingEdges.putIfAbsent(edge.getSrc(), new HashSet<>());
    this.dagOutgoingEdges.get(edge.getSrc()).add(edge);
  }

  /**
   * @return outgoing edges of the contained DAG.
   */
  public Map<IRVertex, Set<IREdge>> getDagOutgoingEdges() {
    return this.dagOutgoingEdges;
  }

  /**
   * Marks duplicate edges with DuplicateEdgeGroupProperty.
   */
  public void markDuplicateEdges() {
    nonIterativeIncomingEdges.forEach(((irVertex, irEdges) -> irEdges.forEach(irEdge -> {
      irEdge.setProperty(
        DuplicateEdgeGroupProperty.of(new DuplicateEdgeGroupPropertyValue(String.valueOf(duplicateEdgeGroupId))));
      duplicateEdgeGroupId++;
    })));
  }

  /**
   * Method for unrolling an iteration of the LoopVertex.
   *
   * @param dagBuilder DAGBuilder to add the unrolled iteration to.
   * @return a LoopVertex with one less maximum iteration.
   */
  public LoopVertex unRollIteration(final DAGBuilder<IRVertex, IREdge> dagBuilder) {
    final HashMap<IRVertex, IRVertex> originalToNewIRVertex = new HashMap<>();
    final DAG<IRVertex, IREdge> dagToAdd = getDAG();

    decreaseMaxNumberOfIterations();

    // add the DAG and internal edges to the dagBuilder.
    dagToAdd.topologicalDo(irVertex -> {
      final IRVertex newIrVertex = irVertex.getClone();
      originalToNewIRVertex.putIfAbsent(irVertex, newIrVertex);

      dagBuilder.addVertex(newIrVertex, dagToAdd);
      dagToAdd.getIncomingEdgesOf(irVertex).forEach(edge -> {
        final IRVertex newSrc = originalToNewIRVertex.get(edge.getSrc());
        final IREdge newIrEdge =
          new IREdge(edge.getPropertyValue(CommunicationPatternProperty.class).get(), newSrc, newIrVertex);
        edge.copyExecutionPropertiesTo(newIrEdge);
        dagBuilder.connectVertices(newIrEdge);
      });
    });

    // process DAG incoming edges.
    getDagIncomingEdges().forEach((dstVertex, irEdges) -> irEdges.forEach(edge -> {
      final IREdge newIrEdge = new IREdge(edge.getPropertyValue(CommunicationPatternProperty.class).get(),
        edge.getSrc(), originalToNewIRVertex.get(dstVertex));
      edge.copyExecutionPropertiesTo(newIrEdge);
      dagBuilder.connectVertices(newIrEdge);
    }));

    if (loopTerminationConditionMet()) {
      // if termination condition met, we process the DAG outgoing edge.
      getDagOutgoingEdges().forEach((srcVertex, irEdges) -> irEdges.forEach(edge -> {
        final IREdge newIrEdge = new IREdge(edge.getPropertyValue(CommunicationPatternProperty.class).get(),
          originalToNewIRVertex.get(srcVertex), edge.getDst());
        edge.copyExecutionPropertiesTo(newIrEdge);
        dagBuilder.addVertex(edge.getDst()).connectVertices(newIrEdge);
      }));
    }

    // process next iteration's DAG incoming edges
    this.getDagIncomingEdges().clear();
    this.nonIterativeIncomingEdges.forEach((dstVertex, irEdges) -> irEdges.forEach(this::addDagIncomingEdge));
    this.iterativeIncomingEdges.forEach((dstVertex, irEdges) -> irEdges.forEach(edge -> {
      final IREdge newIrEdge = new IREdge(edge.getPropertyValue(CommunicationPatternProperty.class).get(),
        originalToNewIRVertex.get(edge.getSrc()), dstVertex);
      edge.copyExecutionPropertiesTo(newIrEdge);
      this.addDagIncomingEdge(newIrEdge);
    }));

    return this;
  }

  /**
   * @return whether or not the loop termination condition has been met.
   */
  public Boolean loopTerminationConditionMet() {
    return loopTerminationConditionMet(maxNumberOfIterations);
  }

  /**
   * @param intPredicateInput input for the intPredicate of the loop termination condition.
   * @return whether or not the loop termination condition has been met.
   */
  public Boolean loopTerminationConditionMet(final Integer intPredicateInput) {
    return maxNumberOfIterations <= 0 || terminationCondition.test(intPredicateInput);
  }

  /**
   * Set the maximum number of iterations.
   *
   * @param maxNum maximum number of iterations.
   */
  public void setMaxNumberOfIterations(final Integer maxNum) {
    this.maxNumberOfIterations = maxNum;
  }

  /**
   * @return termination condition int predicate.
   */
  public IntPredicate getTerminationCondition() {
    return terminationCondition;
  }

  /**
   * @return maximum number of iterations.
   */
  public Integer getMaxNumberOfIterations() {
    return this.maxNumberOfIterations;
  }

  /**
   * increase the value of maximum number of iterations by 1.
   */
  public void increaseMaxNumberOfIterations() {
    this.maxNumberOfIterations++;
  }

  /**
   * decrease the value of maximum number of iterations by 1.
   */
  private void decreaseMaxNumberOfIterations() {
    this.maxNumberOfIterations--;
  }

  /**
   * Check termination condition.
   *
   * @param that another vertex.
   * @return true if equals.
   */
  public boolean terminationConditionEquals(final LoopVertex that) {
    if (this.maxNumberOfIterations.equals(that.getMaxNumberOfIterations()) && Util
      .checkEqualityOfIntPredicates(this.terminationCondition, that.getTerminationCondition(),
        this.maxNumberOfIterations)) {
      return true;
    }
    return false;
  }

  /**
   * Set the intPredicate termination condition for the LoopVertex.
   *
   * @param terminationCondition the termination condition to set.
   */
  public void setTerminationCondition(final IntPredicate terminationCondition) {
    this.terminationCondition = terminationCondition;
  }

  @Override
  public ObjectNode getPropertiesAsJsonNode() {
    final ObjectNode node = getIRVertexPropertiesAsJsonNode();
    node.put("remainingIteration", maxNumberOfIterations);
    node.set("DAG", getDAG().asJsonNode());
    node.set("dagIncomingEdges", crossingEdgesToJSON(dagIncomingEdges));
    node.set("dagOutgoingEdges", crossingEdgesToJSON(dagOutgoingEdges));
    final ObjectNode edgeMappings = node.putObject("edgeWithLoopToEdgeWithInternalVertex");
    edgeWithLoopToEdgeWithInternalVertex.entrySet()
      .forEach(entry -> edgeMappings.put(entry.getKey().getId(), entry.getValue().getId()));
    return node;
  }

  /**
   * Convert the crossing edges to JSON.
   *
   * @param map map of the crossing edges.
   * @return a string of JSON showing the crossing edges.
   */
  private static ObjectNode crossingEdgesToJSON(final Map<IRVertex, Set<IREdge>> map) {
    final ObjectNode node = JsonNodeFactory.instance.objectNode();
    map.forEach(((irVertex, irEdges) -> {
      final ArrayNode vertexNode = node.putArray(irVertex.getId());
      irEdges.forEach(e -> vertexNode.add(e.getId()));
    }));
    return node;
  }
}
