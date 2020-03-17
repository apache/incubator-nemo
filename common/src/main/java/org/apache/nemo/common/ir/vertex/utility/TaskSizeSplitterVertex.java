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
package org.apache.nemo.common.ir.vertex.utility;

import org.apache.nemo.common.HashRange;
import org.apache.nemo.common.KeyRange;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.MessageIdEdgeProperty;
import org.apache.nemo.common.ir.edge.executionproperty.SubPartitionSetProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.LoopVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.MessageIdVertexProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.transform.SignalTransform;
import org.apache.nemo.common.ir.vertex.utility.runtimepasstriggervertex.SignalVertex;
import org.apache.nemo.common.test.EmptyComponents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This vertex works as a partition-based sampling vertex of dynamic task sizing pass.
 * It covers both sampling vertices and optimized vertices known from sampling by iterating same vertices, giving
 * different properties in each iteration.
 */
public final class TaskSizeSplitterVertex extends LoopVertex {
  // Information about original(before splitting) vertices
  private static final Logger LOG = LoggerFactory.getLogger(TaskSizeSplitterVertex.class.getName());
  private final Set<IRVertex> originalVertices;
  // Vertex which has incoming edge from other stages. Guaranteed to be only one in each stage by stage partitioner
  private final Set<IRVertex> firstVerticesInStage;
  // vertices which has outgoing edge to other stages. Can be more than one in one stage
  private final Set<IRVertex> verticesWithStageOutgoingEdges;
  // vertices which does not have any outgoing edge to vertices in same stage
  private final Set<IRVertex> lastVerticesInStage;

  // Information about partition sizes
  private final int partitionerProperty;

  // Information about splitter vertex's iteration
  private int testingTrial;

  private final Map<IRVertex, IRVertex> mapOfOriginalVertexToClone = new HashMap<>();

  /**
   * Default constructor of TaskSizeSplitterVertex.
   * @param splitterVertexName              for now, this doesn't do anything. This is inserted to enable extension
   *                                        from LoopVertex.
   * @param originalVertices                Set of vertices which form one stage and which splitter will wrap up.
   * @param firstVerticesInStage            The first vertex in stage. Although it is given as a Set, we assert that
   *                                        this set has only one element (guaranteed by stage partitioner logic)
   * @param verticesWithStageOutgoingEdges  Vertices which has outgoing edges to other stage.
   * @param lastVerticesInStage             Vertices which has only outgoing edges to other stage.
   * @param partitionerProperty             PartitionerProperty of incoming stage edge regarding to job data size.
   *                                        For more information, check
   */
  public TaskSizeSplitterVertex(final String splitterVertexName,
                                final Set<IRVertex> originalVertices,
                                final Set<IRVertex> firstVerticesInStage,
                                final Set<IRVertex> verticesWithStageOutgoingEdges,
                                final Set<IRVertex> lastVerticesInStage,
                                final int partitionerProperty) {
    super(splitterVertexName); // need to take care of here
    this.testingTrial = 0;
    this.originalVertices = originalVertices;
    this.partitionerProperty = partitionerProperty;
    for (IRVertex original : originalVertices) {
      mapOfOriginalVertexToClone.putIfAbsent(original, original.getClone());
    }
    this.firstVerticesInStage = firstVerticesInStage;
    this.verticesWithStageOutgoingEdges = verticesWithStageOutgoingEdges;
    this.lastVerticesInStage = lastVerticesInStage;
  }

  // Getters of attributes
  public Set<IRVertex> getOriginalVertices() {
    return originalVertices;
  }

  public Set<IRVertex> getFirstVerticesInStage() {
    return firstVerticesInStage;
  }

  public Set<IRVertex> getVerticesWithStageOutgoingEdges() {
    return verticesWithStageOutgoingEdges;
  }

  /**
   * Insert vertices from original dag. This does not harm their topological order.
   * @param stageVertices   vertices to insert. can be same as OriginalVertices.
   * @param edgesInBetween  edges connecting stageVertices. This stage does not contain any edge
   *                        that are connected to vertices other than those in stageVertices.
   *                        (Both ends need to be the element of stageVertices)
   */
  public void insertWorkingVertices(final Set<IRVertex> stageVertices, final Set<IREdge> edgesInBetween) {
    stageVertices.forEach(vertex -> getBuilder().addVertex(vertex));
    edgesInBetween.forEach(edge -> getBuilder().connectVertices(edge));
  }

  /**
   * Inserts signal Vertex at the end of the iteration. Last iteration does not contain any signal vertex.
   * (stage finishing vertices) - dummyShuffleEdge - SignalVertex
   * SignalVertex - ControlEdge - (stage starting vertices)
   * @param toInsert              SignalVertex to insert.
   */
  public void insertSignalVertex(final SignalVertex toInsert) {
    getBuilder().addVertex(toInsert);
    for (IRVertex lastVertex : lastVerticesInStage) {
      IREdge edgeToSignal = EmptyComponents.newDummyShuffleEdge(lastVertex, toInsert);
      getBuilder().connectVertices(edgeToSignal);
      for (IRVertex firstVertex : firstVerticesInStage) {
        IREdge controlEdgeToBeginning = Util.createControlEdge(toInsert, firstVertex);
        addIterativeIncomingEdge(controlEdgeToBeginning);
      }
    }
  }

  public void increaseTestingTrial() {
    testingTrial++;
  }

  /**
   * Need to be careful about Signal Vertex, because they do not appear in the last iteration.
   * @param dagBuilder DAGBuilder to add the unrolled iteration to.
   * @return Modified this object
   */
  public TaskSizeSplitterVertex unRollIteration(final DAGBuilder<IRVertex, IREdge> dagBuilder) {
    final HashMap<IRVertex, IRVertex> originalToNewIRVertex = new HashMap<>();
    final HashSet<IRVertex> originalUtilityVertices = new HashSet<>();
    final HashSet<IREdge> edgesToOptimize = new HashSet<>();

    final List<OperatorVertex> previousSignalVertex = new ArrayList<>(1);
    final DAG<IRVertex, IREdge> dagToAdd = getDAG();

    decreaseMaxNumberOfIterations();

    // add the working vertex and its incoming edges to the dagBuilder.
    dagToAdd.topologicalDo(irVertex -> {
      if (!(irVertex instanceof SignalVertex)) {
        final IRVertex newIrVertex = irVertex.getClone();
        setParallelismPropertyByTestingTrial(newIrVertex);
        originalToNewIRVertex.putIfAbsent(irVertex, newIrVertex);
        dagBuilder.addVertex(newIrVertex, dagToAdd);
        dagToAdd.getIncomingEdgesOf(irVertex).forEach(edge -> {
          final IRVertex newSrc = originalToNewIRVertex.get(edge.getSrc());
          final IREdge newIrEdge =
            new IREdge(edge.getPropertyValue(CommunicationPatternProperty.class).get(), newSrc, newIrVertex);
          edge.copyExecutionPropertiesTo(newIrEdge);
          setSubPartitionSetPropertyByTestingTrial(newIrEdge);
          edgesToOptimize.add(newIrEdge);
          dagBuilder.connectVertices(newIrEdge);
        });
      } else {
        originalUtilityVertices.add(irVertex);
      }
    });

    // process the initial DAG incoming edges for the first loop.
    getDagIncomingEdges().forEach((dstVertex, irEdges) -> irEdges.forEach(edge -> {
      final IREdge newIrEdge = new IREdge(edge.getPropertyValue(CommunicationPatternProperty.class).get(),
        edge.getSrc(), originalToNewIRVertex.get(dstVertex));
      edge.copyExecutionPropertiesTo(newIrEdge);
      setSubPartitionSetPropertyByTestingTrial(newIrEdge);
      if (edge.getSrc() instanceof OperatorVertex
        && ((OperatorVertex) edge.getSrc()).getTransform() instanceof SignalTransform) {
        previousSignalVertex.add((OperatorVertex) edge.getSrc());
      } else {
        edgesToOptimize.add(newIrEdge);
      }
      dagBuilder.connectVertices(newIrEdge);
    }));

    getDagOutgoingEdges().forEach((srcVertex, irEdges) -> irEdges.forEach(edgeFromOriginal -> {
      for (Map.Entry<IREdge, IREdge> entry : this.getEdgeWithInternalVertexToEdgeWithLoop().entrySet()) {
        if (entry.getKey().getId().equals(edgeFromOriginal.getId())) {
          final IREdge correspondingEdge = entry.getValue(); // edge to next splitter vertex
          if (correspondingEdge.getDst() instanceof TaskSizeSplitterVertex) {
            TaskSizeSplitterVertex nextSplitter = (TaskSizeSplitterVertex) correspondingEdge.getDst();
            IRVertex dstVertex = edgeFromOriginal.getDst(); // vertex inside of next splitter vertex
            List<IREdge> edgesToDelete = new ArrayList<>();
            List<IREdge> edgesToAdd = new ArrayList<>();
            for (IREdge edgeToDst : nextSplitter.getDagIncomingEdges().get(dstVertex)) {
              if (edgeToDst.getSrc().getId().equals(srcVertex.getId())) {
                final IREdge newIrEdge = new IREdge(
                  edgeFromOriginal.getPropertyValue(CommunicationPatternProperty.class).get(),
                  originalToNewIRVertex.get(srcVertex),
                  edgeFromOriginal.getDst());
                edgeToDst.copyExecutionPropertiesTo(newIrEdge);
                edgesToDelete.add(edgeToDst);
                edgesToAdd.add(newIrEdge);
                final IREdge newLoopEdge = Util.cloneEdge(
                  correspondingEdge, newIrEdge.getSrc(), correspondingEdge.getDst());
                nextSplitter.mapEdgeWithLoop(newLoopEdge, newIrEdge);
              }
            }
            if (loopTerminationConditionMet()) {
              for (IREdge edgeToDelete : edgesToDelete) {
                nextSplitter.removeDagIncomingEdge(edgeToDelete);
                nextSplitter.removeNonIterativeIncomingEdge(edgeToDelete);
              }
            }
            for (IREdge edgeToAdd : edgesToAdd) {
              nextSplitter.addDagIncomingEdge(edgeToAdd);
              nextSplitter.addNonIterativeIncomingEdge(edgeToAdd);
            }
          } else {
            final IREdge newIrEdge = new IREdge(
              edgeFromOriginal.getPropertyValue(CommunicationPatternProperty.class).get(),
              originalToNewIRVertex.get(srcVertex), edgeFromOriginal.getDst());
            edgeFromOriginal.copyExecutionPropertiesTo(newIrEdge);
            dagBuilder.addVertex(edgeFromOriginal.getDst()).connectVertices(newIrEdge);
          }
        }
      }
    }));

    // if loop termination condition is false, add signal vertex
    if (!loopTerminationConditionMet()) {
      for (IRVertex helper : originalUtilityVertices) {
        final IRVertex newHelper = helper.getClone();
        originalToNewIRVertex.putIfAbsent(helper, newHelper);
        setParallelismPropertyByTestingTrial(newHelper);
        dagBuilder.addVertex(newHelper, dagToAdd);
        dagToAdd.getIncomingEdgesOf(helper).forEach(edge -> {
          final IRVertex newSrc = originalToNewIRVertex.get(edge.getSrc());
          final IREdge newIrEdge =
            new IREdge(edge.getPropertyValue(CommunicationPatternProperty.class).get(), newSrc, newHelper);
          edge.copyExecutionPropertiesTo(newIrEdge);
          dagBuilder.connectVertices(newIrEdge);
        });
      }
    }

    // assign signal vertex of n-th iteration with nonIterativeIncomingEdges of (n+1)th iteration
    markEdgesToOptimize(previousSignalVertex, edgesToOptimize);

    // process next iteration's DAG incoming edges, and add them as the next loop's incoming edges:
    // clear, as we're done with the current loop and need to prepare it for the next one.
    this.getDagIncomingEdges().clear();
    this.getNonIterativeIncomingEdges().forEach((dstVertex, irEdges) -> irEdges.forEach(this::addDagIncomingEdge));
    if (!loopTerminationConditionMet()) {
      this.getIterativeIncomingEdges().forEach((dstVertex, irEdges) -> irEdges.forEach(edge -> {
        final IREdge newIrEdge = new IREdge(edge.getPropertyValue(CommunicationPatternProperty.class).get(),
          originalToNewIRVertex.get(edge.getSrc()), dstVertex);
        edge.copyExecutionPropertiesTo(newIrEdge);
        this.addDagIncomingEdge(newIrEdge);
      }));
    }

    increaseTestingTrial();
    return this;
  }

  // private helper methods

  /**
   * Set Parallelism Property of internal vertices by unroll iteration.
   * @param irVertex    vertex to set parallelism property.
   */
  private void setParallelismPropertyByTestingTrial(final IRVertex irVertex) {
    if (testingTrial == 0 && !(irVertex instanceof OperatorVertex
      && ((OperatorVertex) irVertex).getTransform() instanceof SignalTransform)) {
      irVertex.setPropertyPermanently(ParallelismProperty.of(32));
    } else {
      irVertex.setProperty(ParallelismProperty.of(1));
    }
  }

  /**
   * Set SubPartitionSetProperty of given edge by unroll iteration.
   * @param edge    edge to set subPartitionSetProperty
   */
  private void setSubPartitionSetPropertyByTestingTrial(final IREdge edge) {
    final ArrayList<KeyRange> partitionSet = new ArrayList<>();
    int taskIndex = 0;
    if (testingTrial == 0) {
      for (int i = 0; i < 4; i++) {
        partitionSet.add(taskIndex, HashRange.of(i, i + 1));
        taskIndex++;
      }
      for (int groupStartingIndex = 4; groupStartingIndex < 512; groupStartingIndex *= 2) {
        int growingFactor = groupStartingIndex / 4;
        for (int startIndex = groupStartingIndex; startIndex < groupStartingIndex * 2; startIndex += growingFactor) {
          partitionSet.add(taskIndex, HashRange.of(startIndex, startIndex + growingFactor));
          taskIndex++;
        }
      }
      edge.setProperty(SubPartitionSetProperty.of(partitionSet));
    } else {
      partitionSet.add(0, HashRange.of(512, partitionerProperty)); // 31+testingTrial
      edge.setProperty(SubPartitionSetProperty.of(partitionSet));
    }
  }

  /**
   * Mark edges for DTS (i.e. incoming edges of second iteration vertices).
   * @param toAssign          Signal Vertex to get MessageIdVertexProperty
   * @param edgesToOptimize   Edges to mark for DTS
   */
  private void markEdgesToOptimize(final List<OperatorVertex> toAssign, final Set<IREdge> edgesToOptimize) {
    if (testingTrial > 0) {
      edgesToOptimize.forEach(edge -> {
        if (!edge.getDst().getPropertyValue(ParallelismProperty.class).get().equals(1)) {
          throw new IllegalArgumentException("Target edges should begin with Parallelism of 1.");
        }
        final HashSet<Integer> msgEdgeIds =
          edge.getPropertyValue(MessageIdEdgeProperty.class).orElse(new HashSet<>(0));
        msgEdgeIds.add(toAssign.get(0).getPropertyValue(MessageIdVertexProperty.class).get());
        edge.setProperty(MessageIdEdgeProperty.of(msgEdgeIds));
      });
    }
  }

  public void printLogs() {
    LOG.error("[Vertex] this is splitter {}", this.getId());
    LOG.error("[Vertex] get dag incoming edges: {}", this.getDagIncomingEdges().entrySet());
    LOG.error("[Vertex] get dag iterative incoming edges: {}", this.getIterativeIncomingEdges().entrySet());
    LOG.error("[Vertex] get dag nonIterative incoming edges: {}", this.getNonIterativeIncomingEdges().entrySet());
    LOG.error("[Vertex] get dag outgoing edges: {}", this.getDagOutgoingEdges().entrySet());
    LOG.error("[Vertex] get edge map with loop {}", this.getEdgeWithLoopToEdgeWithInternalVertex().entrySet());
    LOG.error("[Vertex] get edge map with internal vertex {}",
      this.getEdgeWithInternalVertexToEdgeWithLoop().entrySet());
  }
}
