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
package edu.snu.nemo.common.ir.vertex;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupPropertyValue;

import java.io.Serializable;
import java.util.*;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

/**
 * IRVertex that contains a partial DAG that is iterative.
 */
public final class LoopVertex extends IRVertex {
  private static int duplicateEdgeGroupId = 0;
  private final DAGBuilder<IRVertex, IREdge> builder; // Contains DAG information
  private final String compositeTransformFullName;

  private final Map<IRVertex, Set<IREdge>> dagIncomingEdges; // for the initial iteration
  private final Map<IRVertex, Set<IREdge>> iterativeIncomingEdges; // Edges from previous iterations connected internal.
  private final Map<IRVertex, Set<IREdge>> nonIterativeIncomingEdges; // Edges from outside previous iterations.
  private final Map<IRVertex, Set<IREdge>> dagOutgoingEdges; // for the final iteration
  private final Map<IREdge, IREdge> edgeWithLoopToEdgeWithInternalVertex;
  private final Map<IREdge, IREdge> edgeWithInternalVertexToEdgeWithLoop;

  private Integer maxNumberOfIterations;
  private IntPredicate terminationCondition;

  /**
   * The LoopVertex constructor.
   * @param compositeTransformFullName full name of the composite transform.
   */
  public LoopVertex(final String compositeTransformFullName) {
    super();
    this.builder = new DAGBuilder<>();
    this.compositeTransformFullName = compositeTransformFullName;
    this.dagIncomingEdges = new HashMap<>();
    this.iterativeIncomingEdges = new HashMap<>();
    this.nonIterativeIncomingEdges = new HashMap<>();
    this.dagOutgoingEdges = new HashMap<>();
    this.edgeWithLoopToEdgeWithInternalVertex = new HashMap<>();
    this.edgeWithInternalVertexToEdgeWithLoop = new HashMap<>();
    this.maxNumberOfIterations = 1; // 1 is the default number of iterations.
    this.terminationCondition = (IntPredicate & Serializable) (integer -> false); // nothing much yet.
  }

  @Override
  public LoopVertex getClone() {
    final LoopVertex newLoopVertex = new LoopVertex(compositeTransformFullName);

    // Copy all elements to the clone
    final DAG<IRVertex, IREdge> dagToCopy = this.getDAG();
    dagToCopy.topologicalDo(v -> {
      newLoopVertex.getBuilder().addVertex(v, dagToCopy);
      dagToCopy.getIncomingEdgesOf(v).forEach(newLoopVertex.getBuilder()::connectVertices);
    });
    this.dagIncomingEdges.forEach(((v, es) -> es.forEach(newLoopVertex::addDagIncomingEdge)));
    this.iterativeIncomingEdges.forEach((v, es) -> es.forEach(newLoopVertex::addIterativeIncomingEdge));
    this.nonIterativeIncomingEdges.forEach((v, es) -> es.forEach(newLoopVertex::addNonIterativeIncomingEdge));
    this.dagOutgoingEdges.forEach(((v, es) -> es.forEach(newLoopVertex::addDagOutgoingEdge)));
    this.edgeWithLoopToEdgeWithInternalVertex.forEach((eLoop, eInternal)
        -> newLoopVertex.mapEdgeWithLoop(eLoop, eInternal));
    newLoopVertex.setMaxNumberOfIterations(maxNumberOfIterations);
    newLoopVertex.setTerminationCondition(terminationCondition);

    this.copyExecutionPropertiesTo(newLoopVertex);
    return newLoopVertex;
  }

  /**
   * @return DAGBuilder of the LoopVertex.
   */
  public DAGBuilder<IRVertex, IREdge> getBuilder() {
    return builder;
  }

  /**
   * @return the DAG of rthe LoopVertex
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
   * @param edgeWithLoop an edge from/to loop
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
        final IREdge newIrEdge = new IREdge(edge.getPropertyValue(DataCommunicationPatternProperty.class).get(),
            newSrc, newIrVertex, edge.isSideInput());
        edge.copyExecutionPropertiesTo(newIrEdge);
        dagBuilder.connectVertices(newIrEdge);
      });
    });

    // process DAG incoming edges.
    getDagIncomingEdges().forEach((dstVertex, irEdges) -> irEdges.forEach(edge -> {
      final IREdge newIrEdge = new IREdge(edge.getPropertyValue(DataCommunicationPatternProperty.class).get(),
          edge.getSrc(), originalToNewIRVertex.get(dstVertex), edge.isSideInput());
      edge.copyExecutionPropertiesTo(newIrEdge);
      dagBuilder.connectVertices(newIrEdge);
    }));

    if (loopTerminationConditionMet()) {
      // if termination condition met, we process the DAG outgoing edge.
      getDagOutgoingEdges().forEach((srcVertex, irEdges) -> irEdges.forEach(edge -> {
        final IREdge newIrEdge = new IREdge(edge.getPropertyValue(DataCommunicationPatternProperty.class).get(),
            originalToNewIRVertex.get(srcVertex), edge.getDst(), edge.isSideInput());
        edge.copyExecutionPropertiesTo(newIrEdge);
        dagBuilder.addVertex(edge.getDst()).connectVertices(newIrEdge);
      }));
    }

    // process next iteration's DAG incoming edges
    this.getDagIncomingEdges().clear();
    this.nonIterativeIncomingEdges.forEach((dstVertex, irEdges) -> irEdges.forEach(this::addDagIncomingEdge));
    this.iterativeIncomingEdges.forEach((dstVertex, irEdges) -> irEdges.forEach(edge -> {
      final IREdge newIrEdge = new IREdge(edge.getPropertyValue(DataCommunicationPatternProperty.class).get(),
          originalToNewIRVertex.get(edge.getSrc()), dstVertex, edge.isSideInput());
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
   * Set the intPredicate termination condition for the LoopVertex.
   * @param terminationCondition the termination condition to set.
   */
  public void setTerminationCondition(final IntPredicate terminationCondition) {
    this.terminationCondition = terminationCondition;
  }

  @Override
  public String propertiesToJSON() {
    final List<String> edgeMappings = edgeWithLoopToEdgeWithInternalVertex.entrySet().stream()
        .map(entry -> String.format("\"%s\": \"%s\"", entry.getKey().getId(), entry.getValue().getId()))
        .collect(Collectors.toList());
    final StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append(irVertexPropertiesToString());
    sb.append(", \"remainingIteration\": ");
    sb.append(this.maxNumberOfIterations);
    sb.append(", \"DAG\": ");
    sb.append(getDAG());
    sb.append(", \"dagIncomingEdges\": ").append(crossingEdgesToJSON(dagIncomingEdges));
    sb.append(", \"dagOutgoingEdges\": ").append(crossingEdgesToJSON(dagOutgoingEdges));
    sb.append(", \"edgeWithLoopToEdgeWithInternalVertex\": {").append(String.join(", ", edgeMappings));
    sb.append("}}");
    return sb.toString();
  }

  /**
   * Convert the crossing edges to JSON.
   * @param map map of the crossing edges.
   * @return a string of JSON showing the crossing edges.
   */
  private static String crossingEdgesToJSON(final Map<IRVertex, Set<IREdge>> map) {
    final ArrayList<String> vertices = new ArrayList<>();
    map.forEach(((irVertex, irEdges) -> {
      final StringBuilder sb = new StringBuilder();
      sb.append("\"").append(irVertex.getId()).append("\": [");
      final List<String> edges = irEdges.stream().map(e -> "\"" + e.getId() + "\"").collect(Collectors.toList());
      sb.append(String.join(", ", edges)).append("]");
      vertices.add(sb.toString());
    }));
    return "{" + String.join(", ", vertices) + "}";
  }
}
