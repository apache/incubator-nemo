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

import org.apache.commons.lang.mutable.MutableInt;
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
import org.apache.nemo.common.ir.vertex.utility.runtimepass.SignalVertex;
import org.apache.nemo.common.test.EmptyComponents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This vertex works as a partition-based sampling vertex of dynamic task sizing pass.
 * It covers both sampling vertices and optimized vertices known from sampling by iterating same vertices, giving
 * different properties in each iteration.
 */
//TODO 454: Change dependency between LoopVertex and TaskSizeSplitterVertex.
public final class TaskSizeSplitterVertex extends LoopVertex {
  // Information about original(before splitting) vertices
  private static final Logger LOG = LoggerFactory.getLogger(TaskSizeSplitterVertex.class.getName());
  private final Set<IRVertex> originalVertices;
  // Vertex which has incoming edge from other groups. Guaranteed to be only one in each group by stage partitioner
  private final Set<IRVertex> groupStartingVertices;
  // vertices which has outgoing edge to other groups. Can be more than one in one groups
  private final Set<IRVertex> verticesWithGroupOutgoingEdges;
  // vertices which does not have any outgoing edge to vertices in same group
  private final Set<IRVertex> groupEndingVertices;

  // Information about partition sizes
  private final int partitionerProperty;

  // Information about splitter vertex's iteration
  private final MutableInt testingTrial;

  private final Map<IRVertex, IRVertex> mapOfOriginalVertexToClone = new HashMap<>();

  /**
   * Default constructor of TaskSizeSplitterVertex.
   * @param splitterVertexName              for now, this doesn't do anything. This is inserted to enable extension
   *                                        from LoopVertex.
   * @param originalVertices                Set of vertices which form one stage and which splitter will wrap up.
   * @param groupStartingVertices            The first vertex in stage. Although it is given as a Set, we assert that
   *                                        this set has only one element (guaranteed by stage partitioner logic)
   * @param verticesWithGroupOutgoingEdges  Vertices which has outgoing edges to other stage.
   * @param groupEndingVertices             Vertices which has only outgoing edges to other stage.
   * @param edgesBetweenOriginalVertices    Edges which connects original vertices.
   * @param partitionerProperty             PartitionerProperty of incoming stage edge regarding to job data size.
   *                                        For more information, check
   */
  public TaskSizeSplitterVertex(final String splitterVertexName,
                                final Set<IRVertex> originalVertices,
                                final Set<IRVertex> groupStartingVertices,
                                final Set<IRVertex> verticesWithGroupOutgoingEdges,
                                final Set<IRVertex> groupEndingVertices,
                                final Set<IREdge> edgesBetweenOriginalVertices,
                                final int partitionerProperty) {
    super(splitterVertexName); // need to take care of here
    testingTrial = new MutableInt(0);
    this.originalVertices = originalVertices;
    this.partitionerProperty = partitionerProperty;
    for (IRVertex original : originalVertices) {
      mapOfOriginalVertexToClone.putIfAbsent(original, original.getClone());
    }
    this.groupStartingVertices = groupStartingVertices;
    this.verticesWithGroupOutgoingEdges = verticesWithGroupOutgoingEdges;
    this.groupEndingVertices = groupEndingVertices;

    insertWorkingVertices(originalVertices, edgesBetweenOriginalVertices);
    //insertSignalVertex(new SignalVertex());
  }

  // Getters of attributes
  public Set<IRVertex> getOriginalVertices() {
    return originalVertices;
  }

  public Set<IRVertex> getGroupStartingVertices() {
    return groupStartingVertices;
  }

  public Set<IRVertex> getVerticesWithGroupOutgoingEdges() {
    return verticesWithGroupOutgoingEdges;
  }

  public Set<IRVertex> getGroupEndingVertices() {
    return groupEndingVertices;
  }

  /**
   * Insert vertices from original dag. This does not harm their topological order.
   * @param stageVertices   vertices to insert. can be same as OriginalVertices.
   * @param edgesInBetween  edges connecting stageVertices. This stage does not contain any edge
   *                        that are connected to vertices other than those in stageVertices.
   *                        (Both ends need to be the element of stageVertices)
   */
  private void insertWorkingVertices(final Set<IRVertex> stageVertices, final Set<IREdge> edgesInBetween) {
    stageVertices.forEach(vertex -> getBuilder().addVertex(vertex));
    edgesInBetween.forEach(edge -> getBuilder().connectVertices(edge));
  }

  /**
   * Inserts signal Vertex at the end of the iteration. Last iteration does not contain any signal vertex.
   * (stage finishing vertices) - dummyShuffleEdge - SignalVertex
   * SignalVertex - ControlEdge - (stage starting vertices)
   * @param toInsert              SignalVertex to insert.
   */
  private void insertSignalVertex(final SignalVertex toInsert) {
    getBuilder().addVertex(toInsert);
    for (IRVertex lastVertex : groupEndingVertices) {
      IREdge edgeToSignal = EmptyComponents.newDummyShuffleEdge(lastVertex, toInsert);
      getBuilder().connectVertices(edgeToSignal);
      for (IRVertex firstVertex : groupStartingVertices) {
        IREdge controlEdgeToBeginning = Util.createControlEdge(toInsert, firstVertex);
        addIterativeIncomingEdge(controlEdgeToBeginning);
      }
    }
  }

  public void increaseTestingTrial() {
    testingTrial.add(1);
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

    if (testingTrial.intValue() == 0) {
      insertSignalVertex(new SignalVertex());
    }

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
    if (testingTrial.intValue() == 0 && !(irVertex instanceof OperatorVertex
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
    if (testingTrial.intValue() == 0) {
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
   *
   * @param toAssign          Signal Vertex to get MessageIdVertexProperty
   * @param edgesToOptimize   Edges to mark for DTS
   */
  private void markEdgesToOptimize(final List<OperatorVertex> toAssign, final Set<IREdge> edgesToOptimize) {
    if (testingTrial.intValue() > 0) {
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

  // These similar four methods are for inserting TaskSizeSplitterVertex in DAG

  /**
   * Get edges which come to original vertices from outer sources by observing the dag. This will be the
   * 'dagIncomingEdges' in Splitter vertex.
   * Edge case: Happens when previous vertex(i.e. outer source) is also a splitter vertex. In this case, we need to get
   *            original edges which is invisible from the dag by hacking into previous splitter vertex.
   *
   * @param dag     dag to insert Splitter Vertex.
   * @return        a set of edges from outside to original vertices.
   */
  public Set<IREdge> getEdgesFromOutsideToOriginal(final DAG<IRVertex, IREdge> dag) {
    // if previous vertex is splitter vertex, add the last vertex of that splitter vertex in map
    Set<IREdge> fromOutsideToOriginal = new HashSet<>();
    for (IRVertex startingVertex : this.groupStartingVertices) {
      for (IREdge edge : dag.getIncomingEdgesOf(startingVertex)) {
        if (edge.getSrc() instanceof TaskSizeSplitterVertex) {
          for (IRVertex originalInnerSource : ((TaskSizeSplitterVertex) edge.getSrc())
            .getVerticesWithGroupOutgoingEdges()) {
            Set<IREdge> candidates = ((TaskSizeSplitterVertex) edge.getSrc()).
              getDagOutgoingEdges().get(originalInnerSource);
            candidates.stream().filter(edge2 -> edge2.getDst().equals(startingVertex))
              .forEach(fromOutsideToOriginal::add);
          }
        } else {
          fromOutsideToOriginal.add(edge);
        }
      }
    }
    return fromOutsideToOriginal;
  }

  /**
   * Get edges which come from original vertices to outer destinations by observing the dag. This will be the
   * 'dagOutgoingEdges' in Splitter vertex.
   * Edge case: Happens when the vertex to be executed after the splitter vertex (i.e. outer destination)
   *            is also a splitter vertex. In this case, we need to get original edges which is invisible from the dag
   *            by hacking into next splitter vertex.
   *
   * @param dag     dag to insert Splitter Vertex.
   * @return        a set of edges from original vertices to outside.
   */
  public Set<IREdge> getEdgesFromOriginalToOutside(final DAG<IRVertex, IREdge> dag) {
    Set<IREdge> fromOriginalToOutside = new HashSet<>();
    for (IRVertex vertex : verticesWithGroupOutgoingEdges) {
      for (IREdge edge : dag.getOutgoingEdgesOf(vertex)) {
        if (edge.getDst() instanceof TaskSizeSplitterVertex) {
          Set<IRVertex> originalInnerDstVertices = ((TaskSizeSplitterVertex) edge.getDst()).getGroupStartingVertices();
          for (IRVertex innerVertex : originalInnerDstVertices) {
            Set<IREdge> candidates = ((TaskSizeSplitterVertex) edge.getDst()).
              getDagIncomingEdges().get(innerVertex);
            candidates.stream().filter(candidate -> candidate.getSrc().equals(vertex))
              .forEach(fromOriginalToOutside::add);
          }
        } else if (!originalVertices.contains(edge.getDst())) {
          fromOriginalToOutside.add(edge);
        }
      }
    }
    return fromOriginalToOutside;
  }

  /**
   * Get edges which come to splitter from outside sources. These edges have a one-to-one relationship with
   * edgesFromOutsideToOriginal.
   * Edge case: Happens when previous vertex(i.e. outer source) is also a splitter vertex.
   *            In this case, we need to modify the prevSplitter's LoopEdge - InternalEdge mapping relationship,
   *            since inserting this Splitter Vertex changes the destination of prevSplitter's LoopEdge
   *            from the original vertex to this Splitter Vertex
   *
   * @param dag     dag to insert Splitter Vertex
   * @return        a set of edges pointing at Splitter Vertex
   */
  public Set<IREdge> getEdgesFromOutsideToSplitter(final DAG<IRVertex, IREdge> dag) {
    HashSet<IREdge> fromOutsideToSplitter = new HashSet<>();
    for (IRVertex groupStartingVertex : groupStartingVertices) {
      for (IREdge incomingEdge : dag.getIncomingEdgesOf(groupStartingVertex)) {
        if (incomingEdge.getSrc() instanceof TaskSizeSplitterVertex) {
          TaskSizeSplitterVertex prevSplitter = (TaskSizeSplitterVertex) incomingEdge.getSrc();
          IREdge internalEdge = prevSplitter.getEdgeWithInternalVertex(incomingEdge);
          IREdge newIrEdge = Util.cloneEdge(incomingEdge, incomingEdge.getSrc(), this);
          prevSplitter.mapEdgeWithLoop(newIrEdge, internalEdge);
          fromOutsideToSplitter.add(newIrEdge);
        } else {
          IREdge cloneOfIncomingEdge = Util.cloneEdge(incomingEdge, incomingEdge.getSrc(), this);
          fromOutsideToSplitter.add(cloneOfIncomingEdge);
        }
      }
    }
    return fromOutsideToSplitter;
  }

  /**
   * Get edges which come out from splitter to outside destinations. These edges have a one-to-one relationship with
   * edgesFromOriginalToOutside.
   * Edge case: Happens when vertex to be executed after this Splitter Vertex(i.e. outer destination)
   *            is also a Splitter Vertex. In this case, we need to modify the nextSplitter's LoopEdge - InternalEdge
   *            mapping relationship, since inserting this Splitter Vertex changes the source of prevSplitter's
   *            LoopEdge from the original vertex to this Splitter Vertex.
   *
   * @param dag     dag to insert Splitter Vertex.
   * @return        a set of edges coming out from Splitter Vertex.
   */
  public Set<IREdge> getEdgesFromSplitterToOutside(final DAG<IRVertex, IREdge> dag) {
    HashSet<IREdge> fromSplitterToOutside = new HashSet<>();
    for (IRVertex vertex : verticesWithGroupOutgoingEdges) {
      for (IREdge outgoingEdge : dag.getOutgoingEdgesOf(vertex)) {
        if (outgoingEdge.getDst() instanceof TaskSizeSplitterVertex) {
          TaskSizeSplitterVertex nextSplitter = (TaskSizeSplitterVertex) outgoingEdge.getDst();
          IREdge internalEdge = nextSplitter.getEdgeWithInternalVertex(outgoingEdge);
          IREdge newIrEdge = Util.cloneEdge(outgoingEdge, this, outgoingEdge.getDst());
          nextSplitter.mapEdgeWithLoop(newIrEdge, internalEdge);
          fromSplitterToOutside.add(newIrEdge);
        } else if (!originalVertices.contains(outgoingEdge.getDst())) {
          IREdge cloneOfOutgoingEdge = Util.cloneEdge(outgoingEdge, this, outgoingEdge.getDst());
          fromSplitterToOutside.add(cloneOfOutgoingEdge);
        }
      }
    }
    return fromSplitterToOutside;
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
