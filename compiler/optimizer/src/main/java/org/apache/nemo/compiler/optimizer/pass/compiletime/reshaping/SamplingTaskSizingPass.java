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
package org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping;

import org.apache.nemo.common.Util;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.EnableDynamicTaskSizingProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.utility.TaskSizeSplitterVertex;
import org.apache.nemo.common.ir.vertex.utility.runtimepasstriggervertex.SignalVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.Annotates;
import org.apache.nemo.runtime.common.plan.StagePartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Compiler pass for dynamic task size optimization. Happens only when the edge property is SHUFFLE.
 * If (size of given job) >= 1GB: enable dynamic task sizing optimization.
 * else:                          break.
 *
 *
 * @Attributes
 * PARTITIONER_PROPERTY_FOR_SMALL_JOB:  PartitionerProperty for jobs in range of [1GB, 10GB) size.
 * PARTITIONER_PROPERTY_FOR_MEDIUM_JOB: PartitionerProperty for jobs in range of [10GB, 100GB) size.
 * PARTITIONER_PROPERTY_FOR_BIG_JOB:    PartitionerProperty for jobs in range of [100GB, - ) size(No upper limit).
 *
 * source stage - shuffle edge - current stage - next stage
 * -> source stage - [curr stage - signal vertex] - next stage
 * where [] is a splitter vertex
 */
@Annotates({EnableDynamicTaskSizingProperty.class, PartitionerProperty.class, SubPartitionSetProperty.class,
  ParallelismProperty.class})
public final class SamplingTaskSizingPass extends ReshapingPass {
  private static final Logger LOG = LoggerFactory.getLogger(SamplingTaskSizingPass.class.getName());

  private static final int PARTITIONER_PROPERTY_FOR_SMALL_JOB = 1024;
  private static final int PARTITIONER_PROPERTY_FOR_MEDIUM_JOB = 2048;
  private static final int PARTITIONER_PROPERTY_FOR_BIG_JOB = 4096;
  private final StagePartitioner stagePartitioner = new StagePartitioner();

  /**
   * Default constructor.
   */
  public SamplingTaskSizingPass() {
    super(SamplingTaskSizingPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    /* Step 1. check DTS launch by job size */
    boolean enableDynamicTaskSizing = true;
    //getEnableFromJobSize(dag);
    if (!enableDynamicTaskSizing) {
      dag.topologicalDo(v -> {
        v.setProperty(EnableDynamicTaskSizingProperty.of(enableDynamicTaskSizing));
        for (IREdge e : dag.getIncomingEdgesOf(v)) { // do we need this? erase this code and check results
          if (!CommunicationPatternProperty.Value.ONE_TO_ONE.equals(
            e.getPropertyValue(CommunicationPatternProperty.class).get())) {
            final int partitionerProperty = e.getDst().getPropertyValue(ParallelismProperty.class).get();
            e.setPropertyPermanently(PartitionerProperty.of(
              e.getPropertyValue(PartitionerProperty.class).get().left(), partitionerProperty));
          }
        }
      });
      return dag;
    }

    final int partitionerProperty = setPartitionerProperty(dag);

    /* Step 2-1. Group vertices by stage using stage merging logic */
    final Map<IRVertex, Integer> vertexToStageId = stagePartitioner.apply(dag);
    final Map<Integer, Set<IRVertex>> stageIdToStageVertices = new HashMap<>();
    vertexToStageId.forEach((vertex, stageId) -> {
      if (!stageIdToStageVertices.containsKey(stageId)) {
        stageIdToStageVertices.put(stageId, new HashSet<>());
      }
      stageIdToStageVertices.get(stageId).add(vertex);
    });

    /* Step 2-2. Mark stages to insert splitter vertex and get target edges of DTS */
    Set<Integer> stageIdsToInsertSplitter = new HashSet<>();
    Set<IREdge> shuffleEdgesForDTS = new HashSet<>();
    dag.topologicalDo(v -> {
      for (final IREdge edge : dag.getIncomingEdgesOf(v)) {
        if (checkForInsertingSplitterVertex(dag, v, edge, vertexToStageId, stageIdToStageVertices)) {
          stageIdsToInsertSplitter.add(vertexToStageId.get(v));
          shuffleEdgesForDTS.add(edge);
        }
      }
    });

    /* Step 2-3. Change partitioner property for DTS target edges */
    dag.topologicalDo(v -> {
      for (final IREdge edge : dag.getIncomingEdgesOf(v)) {
        if (shuffleEdgesForDTS.contains(edge)) {
          shuffleEdgesForDTS.remove(edge);
          edge.setProperty(PartitionerProperty.of(PartitionerProperty.Type.HASH, partitionerProperty));
          shuffleEdgesForDTS.add(edge);
        }
      }
    });

    dag.topologicalDo(v -> {
      if (stageIdsToInsertSplitter.contains(vertexToStageId.get(v))) {
        v.setProperty(EnableDynamicTaskSizingProperty.of(true));
      } else {
        v.setProperty(EnableDynamicTaskSizingProperty.of(false));
      }
    });

    /* Step 3. Insert Splitter Vertex */
    List<IRVertex> reverseTopologicalOrder = dag.getTopologicalSort();
    Collections.reverse(reverseTopologicalOrder);
    for (IRVertex v : reverseTopologicalOrder) {
      for (final IREdge edge : dag.getOutgoingEdgesOf(v)) {
        if (shuffleEdgesForDTS.contains(edge)) {
          // edge is the incoming edge of observing stage, v is the last vertex of previous stage
          Set<IRVertex> stageVertices = stageIdToStageVertices.get(vertexToStageId.get(edge.getDst()));
          Set<IRVertex> verticesWithStageOutgoingEdges = stageVertices.stream().filter(stageVertex ->
            dag.getOutgoingEdgesOf(stageVertex).stream()
              .map(Edge::getDst).anyMatch(Predicate.not(stageVertices::contains)))
            .collect(Collectors.toSet());
          Set<IRVertex> stageEndingVertices = stageVertices.stream()
            .filter(stageVertex -> dag.getOutgoingEdgesOf(stageVertex).isEmpty()
              || !dag.getOutgoingEdgesOf(stageVertex).stream().map(Edge::getDst).anyMatch(stageVertices::contains))
            .collect(Collectors.toSet());
          final boolean isSourcePartition = stageVertices.stream()
            .flatMap(vertexInPartition -> dag.getIncomingEdgesOf(vertexInPartition).stream())
            .map(Edge::getSrc)
            .allMatch(stageVertices::contains);
          if (isSourcePartition) {
            break;
          }
          makeAndInsertSplitterVertex(dag, stageVertices, Collections.singleton(edge.getDst()),
            verticesWithStageOutgoingEdges, stageEndingVertices, partitionerProperty);
        }
      }
    }
    return dag;
  }

  private boolean getEnableFromJobSize(final IRDAG dag) {
    long jobSizeInBytes = dag.getInputSize();
    return jobSizeInBytes >= 1024 * 1024 * 1024;
  }

  /**
   * should be called after EnableDynamicTaskSizingProperty is declared as true.
   * @param dag   IRDAG to get job input data size from
   * @return      partitioner property regarding job size
   */
  private int setPartitionerProperty(final IRDAG dag) {
    long jobSizeInBytes = dag.getInputSize();
    long jobSizeInGB = jobSizeInBytes / (1024 * 1024 * 1024);
    if (1 <= jobSizeInGB && jobSizeInGB < 10) {
      return PARTITIONER_PROPERTY_FOR_SMALL_JOB;
    } else if (10 <= jobSizeInGB && jobSizeInGB < 100) {
      return PARTITIONER_PROPERTY_FOR_MEDIUM_JOB;
    } else {
      return PARTITIONER_PROPERTY_FOR_BIG_JOB;
    }
  }

  /**
   * Check if stage containing observing Vertex is appropriate for inserting splitter vertex.
   * @param dag                     dag to observe
   * @param observingVertex         observing vertex
   * @param observingEdge           incoming edge of observing vertex
   * @param vertexToStageId         maps vertex to its corresponding stage id
   * @param stageIdToStageVertices  maps stage id to its vertices
   * @return                        true if we can wrap this stage with splitter vertex (i.e. appropriate for DTS)
   */
  private boolean checkForInsertingSplitterVertex(final IRDAG dag,
                                                  final IRVertex observingVertex,
                                                  final IREdge observingEdge,
                                                  final Map<IRVertex, Integer> vertexToStageId,
                                                  final Map<Integer, Set<IRVertex>> stageIdToStageVertices) {
    // If communication property of observing Edge is not shuffle, return false.
    if (!CommunicationPatternProperty.Value.SHUFFLE.equals(
      observingEdge.getPropertyValue(CommunicationPatternProperty.class).get())) {
      return false;
    }
    // if observing Vertex has multiple incoming edges, return false
    if (dag.getIncomingEdgesOf(observingVertex).size() > 1) {
      return false;
    }
    // if source vertex of observing Edge has multiple outgoing edge (that is,
    // has outgoing edges other than observing Edge), return false
    if (dag.getOutgoingEdgesOf(observingEdge.getSrc()).size() > 1) {
      return false;
    }
    // if one of the outgoing edges of stage which contains observing Vertex has communication property of one-to-one,
    // return false.
    // (corner case) if this stage is a sink, return true
    // insert to do: accumulate DTS result by changing o2o stage edge into shuffle
    Set<IRVertex> stageVertices = stageIdToStageVertices.get(vertexToStageId.get(observingVertex));
    Set<IREdge> stageOutgoingEdges = stageVertices
      .stream()
      .flatMap(vertex -> dag.getOutgoingEdgesOf(vertex).stream())
      .filter(edge -> !stageVertices.contains(edge.getDst()))
      .collect(Collectors.toSet());
    if (stageOutgoingEdges.isEmpty()) {
      return true;
    } else {
      for (IREdge edge : stageOutgoingEdges) {
        if (CommunicationPatternProperty.Value.ONE_TO_ONE.equals(
          edge.getPropertyValue(CommunicationPatternProperty.class).get())) {
          return false;
        }
      }
    }
    // all cases passed: return true
    return true;
  }

  /**
   * get and set edges which come to stage vertices from outer sources by observing the dag. This will be the
   * 'dagIncomingEdges' in Splitter vertex.
   * Edge case: Happens when previous vertex(i.e. outer source) is also a splitter vertex. In this case, we need to get
   *            original edges which is invisible from the dag by hacking into previous splitter vertex.
   * @param dag                    dag to observe
   * @param stageStartingVertices  stage starting vertices (assumed to be always in size 1)
   * @return                       edges from outside to stage vertices
   */
  private Set<IREdge> setEdgesFromOutsideToOriginal(final IRDAG dag,
                                                    final Set<IRVertex> stageStartingVertices) {
    // if previous vertex is splitter vertex, add the last vertex of that splitter vertex in map
    Set<IREdge> fromOutsideToOriginal = new HashSet<>();
    for (IRVertex startingVertex : stageStartingVertices) {
      for (IREdge edge : dag.getIncomingEdgesOf(startingVertex)) {
        if (edge.getSrc() instanceof TaskSizeSplitterVertex) {
          for (IRVertex originalInnerSource : ((TaskSizeSplitterVertex) edge.getSrc())
            .getVerticesWithStageOutgoingEdges()) {
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

  private Set<IREdge> setEdgesFromOriginalToOutside(final IRDAG dag,
                                                    final Set<IRVertex> stageVertices,
                                                    final Set<IRVertex> verticesWithStageOutgoingEdges) {
    Set<IREdge> fromOriginalToOutside = new HashSet<>();
    for (IRVertex vertex : verticesWithStageOutgoingEdges) {
      for (IREdge edge : dag.getOutgoingEdgesOf(vertex)) {
        if (edge.getDst() instanceof TaskSizeSplitterVertex) {
          Set<IRVertex> originalInnerDstVertices = ((TaskSizeSplitterVertex) edge.getDst()).getFirstVerticesInStage();
          for (IRVertex innerVertex : originalInnerDstVertices) {
            Set<IREdge> candidates = ((TaskSizeSplitterVertex) edge.getDst()).
              getDagIncomingEdges().get(innerVertex);
            candidates.stream().filter(edge2 -> edge2.getSrc().equals(vertex))
              .forEach(fromOriginalToOutside::add);
          }
        } else if (!stageVertices.contains(edge.getDst())) {
          fromOriginalToOutside.add(edge);
        }
      }
    }
    return fromOriginalToOutside;
  }

  /**
   * set edges which come to splitter from outside sources. These edges have a one-to-one relationship with
   * edgesFromOutsideToOriginal.
   * @param dag                   dag to observe
   * @param toInsert              splitter vertex to insert
   * @param stageOpeningVertices  stage opening vertices
   * @return                      set of edges pointing at splitter vertex
   */
  private Set<IREdge> setEdgesFromOutsideToSplitter(final IRDAG dag,
                                                    final TaskSizeSplitterVertex toInsert,
                                                    final Set<IRVertex> stageOpeningVertices) {
    HashSet<IREdge> fromOutsideToSplitter = new HashSet<>();
    for (IRVertex stageOpeningVertex : stageOpeningVertices) {
      for (IREdge incomingEdge : dag.getIncomingEdgesOf(stageOpeningVertex)) {
        if (incomingEdge.getSrc() instanceof TaskSizeSplitterVertex) {
          TaskSizeSplitterVertex prevSplitter = (TaskSizeSplitterVertex) incomingEdge.getSrc();
          IREdge internalEdge = prevSplitter.getEdgeWithInternalVertex(incomingEdge);
          IREdge newIrEdge = Util.cloneEdge(incomingEdge, incomingEdge.getSrc(), toInsert);
          prevSplitter.mapEdgeWithLoop(newIrEdge, internalEdge);
          fromOutsideToSplitter.add(newIrEdge);
        } else {
          IREdge cloneOfIncomingEdge = Util.cloneEdge(incomingEdge, incomingEdge.getSrc(), toInsert);
          fromOutsideToSplitter.add(cloneOfIncomingEdge);
        }
      }
    }
    return fromOutsideToSplitter;
  }

  /**
   * Set edges which go out from splitter vertex to other stages.
   * @param dag                             dag to modify
   * @param toInsert                        splitter vertex to check
   * @param verticesWithStageOutgoingEdges  vertices with stage outgoing edges
   * @return
   */
  private Set<IREdge> setEdgesFromSplitterToOutside(final IRDAG dag,
                                                    final TaskSizeSplitterVertex toInsert,
                                                    final Set<IRVertex> verticesWithStageOutgoingEdges) {
    HashSet<IREdge> fromSplitterToOutside = new HashSet<>();
    for (IRVertex vertex : verticesWithStageOutgoingEdges) {
      for (IREdge outgoingEdge : dag.getOutgoingEdgesOf(vertex)) {
        if (outgoingEdge.getDst() instanceof TaskSizeSplitterVertex) {
          TaskSizeSplitterVertex nextSplitter = (TaskSizeSplitterVertex) outgoingEdge.getDst();
          IREdge internalEdge = nextSplitter.getEdgeWithInternalVertex(outgoingEdge);
          IREdge newIrEdge = Util.cloneEdge(outgoingEdge, toInsert, outgoingEdge.getDst());
          nextSplitter.mapEdgeWithLoop(newIrEdge, internalEdge);
          fromSplitterToOutside.add(newIrEdge);
        } else if (!toInsert.getOriginalVertices().contains(outgoingEdge.getDst())) {
          IREdge cloneOfOutgoingEdge = Util.cloneEdge(outgoingEdge, toInsert, outgoingEdge.getDst());
          fromSplitterToOutside.add(cloneOfOutgoingEdge);
        }
      }
    }
    return fromSplitterToOutside;
  }

  /**
   * Make splitter vertex and insert it in the dag.
   * @param dag                              dag to insert splitter vertex
   * @param stageVertices                    stage vertices which will be grouped to be inserted into splitter vertex
   * @param stageStartingVertices            subset of stage vertices which have incoming edge from other stages
   * @param verticesWithStageOutgoingEdges   subset of stage vertices which have outgoing edge to other stages
   * @param stageEndingVertices              subset of staae vertices which does not have outgoing edge to other
   *                                         vertices in this stage
   * @param partitionerProperty              partitioner property
   */
  private void makeAndInsertSplitterVertex(final IRDAG dag,
                                           final Set<IRVertex> stageVertices,
                                           final Set<IRVertex> stageStartingVertices,
                                           final Set<IRVertex> verticesWithStageOutgoingEdges,
                                           final Set<IRVertex> stageEndingVertices,
                                           final int partitionerProperty) {
    final Set<IREdge> incomingEdgesOfOriginalVertices = stageVertices
      .stream()
      .flatMap(ov -> dag.getIncomingEdgesOf(ov).stream())
      .collect(Collectors.toSet());
    final Set<IREdge> outgoingEdgesOfOriginalVertices = stageVertices
      .stream()
      .flatMap(ov -> dag.getOutgoingEdgesOf(ov).stream())
      .collect(Collectors.toSet());
    final Set<IREdge> edgesBetweenOriginalVertices = stageVertices
      .stream()
      .flatMap(ov -> dag.getIncomingEdgesOf(ov).stream())
      .filter(edge -> stageVertices.contains(edge.getSrc()))
      .collect(Collectors.toSet());
    final Set<IREdge> fromOutsideToOriginal = setEdgesFromOutsideToOriginal(dag, stageStartingVertices);
    final Set<IREdge> fromOriginalToOutside = setEdgesFromOriginalToOutside(dag, stageVertices,
      verticesWithStageOutgoingEdges);
    final TaskSizeSplitterVertex toInsert = new TaskSizeSplitterVertex(
      "Splitter" + stageStartingVertices.iterator().next().getId(), stageVertices,
      stageStartingVertices, verticesWithStageOutgoingEdges, stageEndingVertices, partitionerProperty);
    // By default, set the number of iterations as 2
    toInsert.setMaxNumberOfIterations(2);
    // make edges connected to splitter vertex
    final Set<IREdge> fromOutsideToSplitter = setEdgesFromOutsideToSplitter(dag, toInsert, stageStartingVertices);
    final Set<IREdge> fromSplitterToOutside = setEdgesFromSplitterToOutside(dag, toInsert,
      verticesWithStageOutgoingEdges);
    final Set<IREdge> edgesWithSplitterVertex = new HashSet<>();
    edgesWithSplitterVertex.addAll(fromOutsideToSplitter);
    edgesWithSplitterVertex.addAll(fromSplitterToOutside);
    //fill in splitter vertex information
    toInsert.insertWorkingVertices(stageVertices, edgesBetweenOriginalVertices);

    //map splitter vertex connection to corresponding internal vertex connection
    for (IREdge splitterEdge : fromSplitterToOutside) {
      for (IREdge internalEdge : fromOriginalToOutside) {
        if (splitterEdge.getDst() instanceof TaskSizeSplitterVertex) {
          TaskSizeSplitterVertex nextSplitter = (TaskSizeSplitterVertex) splitterEdge.getDst();
          if (nextSplitter.getOriginalVertices().contains(internalEdge.getDst())) {
            toInsert.mapEdgeWithLoop(splitterEdge, internalEdge);
          }
        } else {
          if (splitterEdge.getDst().equals(internalEdge.getDst())) {
            toInsert.mapEdgeWithLoop(splitterEdge, internalEdge);
          }
        }
      }
    }
    for (IREdge splitterEdge : fromOutsideToSplitter) {
      for (IREdge internalEdge : fromOutsideToOriginal) {
        if (splitterEdge.getSrc().equals(internalEdge.getSrc())) {
          toInsert.mapEdgeWithLoop(splitterEdge, internalEdge);
        }
      }
    }

    final SignalVertex signalVertex = new SignalVertex();
    fromOutsideToOriginal.forEach(toInsert::addDagIncomingEdge);
    fromOutsideToOriginal.forEach(toInsert::addNonIterativeIncomingEdge); //
    fromOriginalToOutside.forEach(toInsert::addDagOutgoingEdge);
    toInsert.insertSignalVertex(signalVertex);

    // insert splitter vertex
    dag.insert(toInsert, incomingEdgesOfOriginalVertices, outgoingEdgesOfOriginalVertices,
      edgesWithSplitterVertex);

    toInsert.printLogs();
  }
}
