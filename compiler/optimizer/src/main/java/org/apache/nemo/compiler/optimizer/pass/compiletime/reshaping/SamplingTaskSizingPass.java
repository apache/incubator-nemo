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

import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.EnableDynamicTaskSizingProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.utility.TaskSizeSplitterVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.Annotates;
import org.apache.nemo.runtime.common.plan.StagePartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Compiler pass for dynamic task size optimization. Happens only when the edge property is SHUFFLE.
 * If (size of given job) is greater than or equal to 1GB: enable dynamic task sizing optimization.
 * else:                          break.
 *
 *
 * With attributes
 * PARTITIONER_PROPERTY_FOR_SMALL_JOB:  PartitionerProperty for jobs in range of [1GB, 10GB) size.
 * PARTITIONER_PROPERTY_FOR_MEDIUM_JOB: PartitionerProperty for jobs in range of [10GB, 100GB) size.
 * PARTITIONER_PROPERTY_FOR_BIG_JOB:    PartitionerProperty for jobs in range of [100GB, - ) size(No upper limit).
 *
 * source stage - shuffle edge - current stage - next stage
 * - source stage - [curr stage - signal vertex] - next stage
 * where [] is a splitter vertex
 */
@Annotates({EnableDynamicTaskSizingProperty.class, PartitionerProperty.class, SubPartitionSetProperty.class,
  ParallelismProperty.class})
public final class SamplingTaskSizingPass extends ReshapingPass {
  private static final Logger LOG = LoggerFactory.getLogger(SamplingTaskSizingPass.class.getName());

  private static final int PARTITIONER_PROPERTY_FOR_SMALL_JOB = 1024;
  private static final int PARTITIONER_PROPERTY_FOR_MEDIUM_JOB = 2048;
  private static final int PARTITIONER_PROPERTY_FOR_LARGE_JOB = 4096;
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
    boolean enableDynamicTaskSizing = isDTSEnabledByJobSize(dag);
    if (!enableDynamicTaskSizing) {
      return dag;
    } else {
      dag.topologicalDo(v -> v.setProperty(EnableDynamicTaskSizingProperty.of(enableDynamicTaskSizing)));
    }

    final int partitionerProperty = getPartitionerPropertyByJobSize(dag);

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
        if (isAppropriateForInsertingSplitterVertex(dag, v, edge, vertexToStageId, stageIdToStageVertices)) {
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
    /* Step 3. Insert Splitter Vertex */
    List<IRVertex> reverseTopologicalOrder = dag.getTopologicalSort();
    Collections.reverse(reverseTopologicalOrder);
    for (IRVertex v : reverseTopologicalOrder) {
      for (final IREdge edge : dag.getOutgoingEdgesOf(v)) {
        if (shuffleEdgesForDTS.contains(edge)) {
          // edge is the incoming edge of observing stage, v is the last vertex of previous stage
          Set<IRVertex> stageVertices = stageIdToStageVertices.get(vertexToStageId.get(edge.getDst()));
          Set<IRVertex> verticesWithStageOutgoingEdges = new HashSet<>();
          for (IRVertex v2 : stageVertices) {
            Set<IRVertex> nextVertices = dag.getOutgoingEdgesOf(v2).stream().map(Edge::getDst)
              .collect(Collectors.toSet());
            for (IRVertex v3 : nextVertices) {
              if (!stageVertices.contains(v3)) {
                verticesWithStageOutgoingEdges.add(v2);
              }
            }
          }
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
          insertSplitterVertex(dag, stageVertices, Collections.singleton(edge.getDst()),
            verticesWithStageOutgoingEdges, stageEndingVertices, partitionerProperty);
        }
      }
    }
    return dag;
  }

  private boolean isDTSEnabledByJobSize(final IRDAG dag) {
    long jobSizeInBytes = dag.getInputSize();
    return jobSizeInBytes >= 1024 * 1024 * 1024;
  }

  /**
   * Should be called after EnableDynamicTaskSizingProperty is declared as true.
   * @param dag   IRDAG to get job input data size from
   * @return      partitioner property regarding job size
   */
  private int getPartitionerPropertyByJobSize(final IRDAG dag) {
    long jobSizeInBytes = dag.getInputSize();
    long jobSizeInGB = jobSizeInBytes / (1024 * 1024 * 1024);
    if (1 <= jobSizeInGB && jobSizeInGB < 10) {
      return PARTITIONER_PROPERTY_FOR_SMALL_JOB;
    } else if (10 <= jobSizeInGB && jobSizeInGB < 100) {
      return PARTITIONER_PROPERTY_FOR_MEDIUM_JOB;
    } else {
      return PARTITIONER_PROPERTY_FOR_LARGE_JOB;
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
  private boolean isAppropriateForInsertingSplitterVertex(final IRDAG dag,
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
   * Make splitter vertex and insert it in the dag.
   * @param dag                              dag to insert splitter vertex
   * @param stageVertices                    stage vertices which will be grouped to be inserted into splitter vertex
   * @param stageStartingVertices            subset of stage vertices which have incoming edge from other stages
   * @param verticesWithStageOutgoingEdges   subset of stage vertices which have outgoing edge to other stages
   * @param stageEndingVertices              subset of staae vertices which does not have outgoing edge to other
   *                                         vertices in this stage
   * @param partitionerProperty              partitioner property
   */
  private void insertSplitterVertex(final IRDAG dag,
                                    final Set<IRVertex> stageVertices,
                                    final Set<IRVertex> stageStartingVertices,
                                    final Set<IRVertex> verticesWithStageOutgoingEdges,
                                    final Set<IRVertex> stageEndingVertices,
                                    final int partitionerProperty) {

    final Set<IREdge> edgesBetweenOriginalVertices = stageVertices
      .stream()
      .flatMap(ov -> dag.getIncomingEdgesOf(ov).stream())
      .filter(edge -> stageVertices.contains(edge.getSrc()))
      .collect(Collectors.toSet());

    final TaskSizeSplitterVertex toInsert = new TaskSizeSplitterVertex(
      "Splitter" + stageStartingVertices.iterator().next().getId(),
      stageVertices,
      stageStartingVertices,
      verticesWithStageOutgoingEdges,
      stageEndingVertices,
      edgesBetweenOriginalVertices,
      partitionerProperty);

    // By default, set the number of iterations as 2
    toInsert.setMaxNumberOfIterations(2);

    // insert splitter vertex
    dag.insert(toInsert);

    toInsert.printLogs();
  }

  /**
   * Changes stage outgoing edges' execution property from one-to-one to shuffle when stage incoming edge became the
   * target of DTS.
   * Need to be careful about referenceShuffleEdge because this code does not check whether it is a valid shuffle edge
   * or not.
   * @param edge                   edge to change execution property.
   * @param referenceShuffleEdge  reference shuffle edge to copy key related execution properties
   * @param partitionerProperty   partitioner property of shuffle
   */
  //TODO #452: Allow changing Communication Property of Edge from one-to-one to shuffle.
  private IREdge changeOneToOneEdgeToShuffleEdge(final IREdge edge,
                                                 final IREdge referenceShuffleEdge,
                                                 final int partitionerProperty) {
    //double check
    if (!CommunicationPatternProperty.Value.ONE_TO_ONE.equals(
      edge.getPropertyValue(CommunicationPatternProperty.class).get())
      || !CommunicationPatternProperty.Value.SHUFFLE.equals(
      referenceShuffleEdge.getPropertyValue(CommunicationPatternProperty.class).get())) {
      return edge;
    }

    // properties related to data
    edge.setProperty(CommunicationPatternProperty.of(CommunicationPatternProperty.Value.SHUFFLE));
    edge.setProperty(DataFlowProperty.of(DataFlowProperty.Value.PULL));
    edge.setProperty(PartitionerProperty.of(PartitionerProperty.Type.HASH, partitionerProperty));
    edge.setProperty(DataStoreProperty.of(DataStoreProperty.Value.LOCAL_FILE_STORE));

    // properties related to key
    if (!edge.getPropertyValue(KeyExtractorProperty.class).isPresent()) {
      edge.setProperty(KeyExtractorProperty.of(
        referenceShuffleEdge.getPropertyValue(KeyExtractorProperty.class).get()));
    }
    if (!edge.getPropertyValue(KeyEncoderProperty.class).isPresent()) {
      edge.setProperty(KeyEncoderProperty.of(
        referenceShuffleEdge.getPropertyValue(KeyEncoderProperty.class).get()));
    }
    if (!edge.getPropertyValue(KeyDecoderProperty.class).isPresent()) {
      edge.setProperty(KeyDecoderProperty.of(
        referenceShuffleEdge.getPropertyValue(KeyDecoderProperty.class).get()));
    }
    return edge;
  }
}
