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
package org.apache.nemo.compiler.backend.nemo;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.exception.IllegalVertexOperationException;
import org.apache.nemo.common.exception.PhysicalPlanGenerationException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupPropertyValue;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ScheduleGroupProperty;
import org.apache.nemo.common.ir.vertex.utility.SamplingVertex;
import org.apache.nemo.compiler.backend.Backend;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.*;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Backend component for Nemo Runtime.
 */
public final class NemoBackend implements Backend<PhysicalPlan> {
  private static final Logger LOG = LoggerFactory.getLogger(NemoBackend.class.getName());

  private final String dagDirectory;

  @Inject
  private NemoBackend(@Parameter(JobConf.DAGDirectory.class) final String dagDirectory) {
    this.dagDirectory = dagDirectory;
  }

  @Override
  public PhysicalPlan compile(final IRDAG irDAG, final Function<Object, Optional<String>> existingIdFetcher) {
    // first, stage-partition the IR DAG.
    final DAG<Stage, StageEdge> dagOfStages = stagePartitionIrDAG(irDAG, existingIdFetcher);

    // Sanity check
    dagOfStages.getVertices().forEach(this::integrityCheck);

    // this is needed because of DuplicateEdgeGroupProperty.
    handleDuplicateEdgeGroupProperty(dagOfStages);

    // for debugging purposes.
    dagOfStages.storeJSON(dagDirectory, "plan-logical", "logical execution plan");

    return new PhysicalPlan(RuntimeIdManager.generatePhysicalPlanId(), dagOfStages);
  }

  /**
   * Convert the edge id of DuplicateEdgeGroupProperty to physical edge id.
   *
   * @param dagOfStages dag to manipulate
   */
  private void handleDuplicateEdgeGroupProperty(final DAG<Stage, StageEdge> dagOfStages) {
    final Map<String, List<StageEdge>> edgeGroupToIrEdge = new HashMap<>();

    dagOfStages.topologicalDo(irVertex -> dagOfStages.getIncomingEdgesOf(irVertex).forEach(e -> {
      final Optional<DuplicateEdgeGroupPropertyValue> duplicateEdgeGroupProperty =
        e.getPropertyValue(DuplicateEdgeGroupProperty.class);
      if (duplicateEdgeGroupProperty.isPresent()) {
        final String duplicateGroupId = duplicateEdgeGroupProperty.get().getGroupId();
        edgeGroupToIrEdge.computeIfAbsent(duplicateGroupId, k -> new ArrayList<>()).add(e);
      }
    }));

    edgeGroupToIrEdge.forEach((id, edges) -> {
      final StageEdge firstEdge = edges.get(0);
      final DuplicateEdgeGroupPropertyValue firstDuplicateEdgeValue =
        firstEdge.getPropertyValue(DuplicateEdgeGroupProperty.class).get();

      edges.forEach(e -> {
        final DuplicateEdgeGroupPropertyValue duplicateEdgeGroupProperty =
          e.getPropertyValue(DuplicateEdgeGroupProperty.class).get();
        if (firstDuplicateEdgeValue.isRepresentativeEdgeDecided()) {
          duplicateEdgeGroupProperty.setRepresentativeEdgeId(firstDuplicateEdgeValue.getRepresentativeEdgeId());
        } else {
          duplicateEdgeGroupProperty.setRepresentativeEdgeId(firstEdge.getId());
        }
        duplicateEdgeGroupProperty.setGroupSize(edges.size());
      });
    });
  }

  /**
   * Translates an ir DAG into a stage DAG.
   *
   * ID MANAGEMENT for handling run-time optimizations (re-compilations)
   * - Stage: If there is an existing id then use it, if not then generate a new id
   * - StageEdge: Always inherits the corresponding IREdge id
   *
   * @param irDAG to translate.
   * @param existingIdFetcher for looking up existing ids.
   * @return the DAG composed of stages and stage edges.
   */
  private DAG<Stage, StageEdge> stagePartitionIrDAG(final IRDAG irDAG,
                                                    final Function<Object, Optional<String>> existingIdFetcher) {
    final StagePartitioner stagePartitioner = new StagePartitioner();
    final Map<IRVertex, Integer> vertexToPartitionIdMap = stagePartitioner.apply(irDAG);
    final Map<Integer, Set<IRVertex>> vertexSetForEachPartition = new LinkedHashMap<>();
    irDAG.topologicalDo(irVertex -> {
      final int stageId = vertexToPartitionIdMap.get(irVertex);
      if (!vertexSetForEachPartition.containsKey(stageId)) {
        vertexSetForEachPartition.put(stageId, new HashSet<>());
      }
      vertexSetForEachPartition.get(stageId).add(irVertex);
    });

    final Random random = new Random(hashCode()); // to produce same results for same input IRDAGs
    final Set<IREdge> interStageEdges = new HashSet<>();
    final DAGBuilder<Stage, StageEdge> dagOfStagesBuilder = new DAGBuilder<>();
    final Map<Integer, Stage> partitionIdToStageMap = new HashMap<>();
    final HashSet<IRVertex> isStagePartitioned = new HashSet<>();
    for (final Map.Entry<Integer, Set<IRVertex>> partitionIdAndVertices : vertexSetForEachPartition.entrySet()) {
      final Set<IRVertex> partitionVertices = partitionIdAndVertices.getValue();

      // ID MANAGEMENT: PREFER USING THE EXISTING ID
      final String stageId = existingIdFetcher.apply(partitionVertices).orElse(RuntimeIdManager.generateStageId());

      final ExecutionPropertyMap<VertexExecutionProperty> stageProperties = new ExecutionPropertyMap<>(stageId);
      stagePartitioner.getStageProperties(partitionVertices.iterator().next()).forEach(stageProperties::put);
      final int stageParallelism = stageProperties.get(ParallelismProperty.class)
        .orElseThrow(() -> new RuntimeException("Parallelism property must be set for Stage"));
      final List<Integer> taskIndices = getTaskIndicesToExecute(partitionVertices, stageParallelism, random);
      final DAGBuilder<IRVertex, RuntimeEdge<IRVertex>> stageInternalDAGBuilder = new DAGBuilder<>();

      // Prepare vertexIdToReadables
      final List<Map<String, Readable>> vertexIdToReadables = new ArrayList<>(stageParallelism);
      for (int i = 0; i < stageParallelism; i++) {
        vertexIdToReadables.add(new HashMap<>());
      }

      // For each IRVertex,
      for (final IRVertex v : partitionVertices) {
        final IRVertex vertexToPutIntoStage = getActualVertexToPutIntoStage(v);

        // Take care of the readables of a source vertex.
        if (vertexToPutIntoStage instanceof SourceVertex && !isStagePartitioned.contains(vertexToPutIntoStage)) {
          final SourceVertex sourceVertex = (SourceVertex) vertexToPutIntoStage;
          try {
            final List<Readable> readables = sourceVertex.getReadables(stageParallelism);
            for (int i = 0; i < stageParallelism; i++) {
              vertexIdToReadables.get(i).put(vertexToPutIntoStage.getId(), readables.get(i));
            }
          } catch (final Exception e) {
            throw new PhysicalPlanGenerationException(e);
          }
          // Clear internal metadata.
          sourceVertex.clearInternalStates();
        }

        // Add vertex to the stage.
        stageInternalDAGBuilder.addVertex(vertexToPutIntoStage);
      }

      for (final IRVertex dstVertex : partitionVertices) {
        // Connect all the incoming edges for the vertex.
        irDAG.getIncomingEdgesOf(dstVertex).forEach(irEdge -> {
          final IRVertex srcVertex = irEdge.getSrc();

          // both vertices are in the same stage.
          if (vertexToPartitionIdMap.get(srcVertex).equals(vertexToPartitionIdMap.get(dstVertex))) {
            stageInternalDAGBuilder.connectVertices(new RuntimeEdge<>(
              irEdge.getId(), // ID MANAGEMENT: INHERIT THE IREDGE ID
              irEdge.getExecutionProperties(),
              getActualVertexToPutIntoStage(irEdge.getSrc()),
              getActualVertexToPutIntoStage(irEdge.getDst())));
          } else { // edge comes from another stage
            interStageEdges.add(irEdge);
          }
        });
      }
      // If this runtime stage contains at least one vertex, build it!
      if (!stageInternalDAGBuilder.isEmpty()) {
        final DAG<IRVertex, RuntimeEdge<IRVertex>> stageInternalDAG
          = stageInternalDAGBuilder.buildWithoutSourceSinkCheck();
        final Stage stage = new Stage(
          stageId,
          taskIndices,
          stageInternalDAG,
          stageProperties,
          vertexIdToReadables);
        dagOfStagesBuilder.addVertex(stage);
        partitionIdToStageMap.put(partitionIdAndVertices.getKey(), stage);
      }

      // To prevent re-fetching readables in source vertex
      // during re-generation of physical plan for dynamic optimization.
      isStagePartitioned.addAll(partitionVertices);
    }

    // Add StageEdges
    for (final IREdge interStageEdge : interStageEdges) {
      final Stage srcStage = partitionIdToStageMap.get(vertexToPartitionIdMap.get(interStageEdge.getSrc()));
      final Stage dstStage = partitionIdToStageMap.get(vertexToPartitionIdMap.get(interStageEdge.getDst()));
      if (srcStage == null || dstStage == null) {
        throw new IllegalVertexOperationException(String.format("Stage not added to the builder:%s%s",
          srcStage == null ? String.format(" source stage for %s", interStageEdge.getSrc()) : "",
          dstStage == null ? String.format(" destination stage for %s", interStageEdge.getDst()) : ""));
      }
      dagOfStagesBuilder.connectVertices(new StageEdge(interStageEdge.getId(), interStageEdge.getExecutionProperties(),
        getActualVertexToPutIntoStage(interStageEdge.getSrc()), getActualVertexToPutIntoStage(interStageEdge.getDst()),
        srcStage, dstStage));
    }

    return dagOfStagesBuilder.build();
  }

  /**
   * This method is needed, because we do not want to put Sampling vertices into a stage.
   * The underlying runtime only understands Source and Operator vertices.
   */
  private IRVertex getActualVertexToPutIntoStage(final IRVertex irVertex) {
    return irVertex instanceof SamplingVertex
      ? ((SamplingVertex) irVertex).getCloneOfOriginalVertex()
      : irVertex;
  }

  /**
   * Randomly select task indices for Sampling vertices.
   * Select all task indices for non-Sampling vertices.
   */
  private List<Integer> getTaskIndicesToExecute(final Set<IRVertex> vertices,
                                                final int stageParallelism,
                                                final Random random) {
    if (vertices.stream().map(v -> v instanceof SamplingVertex).collect(Collectors.toSet()).size() != 1) {
      throw new IllegalArgumentException("Must be either all sampling vertices, or none: " + vertices.toString());
    }

    if (vertices.iterator().next() instanceof SamplingVertex) {
      // Use min of the desired sample rates
      final float minSampleRate = vertices.stream()
        .map(v -> ((SamplingVertex) v).getDesiredSampleRate())
        .reduce(BinaryOperator.minBy(Float::compareTo))
        .orElseThrow(() -> new IllegalArgumentException(vertices.toString()));

      // Compute and return indices
      final int numOfTaskIndices = (int) Math.ceil(stageParallelism * minSampleRate);
      final List<Integer> randomIndices = IntStream.range(0, stageParallelism).boxed().collect(Collectors.toList());
      Collections.shuffle(randomIndices, random);
      return new ArrayList<>(randomIndices.subList(0, numOfTaskIndices)); // subList is not serializable.
    } else {
      return IntStream.range(0, stageParallelism).boxed().collect(Collectors.toList());
    }
  }

  /**
   * Integrity check for Stage.
   * @param stage to check for
   */
  private void integrityCheck(final Stage stage) {
    stage.getPropertyValue(ParallelismProperty.class)
      .orElseThrow(() -> new RuntimeException("Parallelism property must be set for Stage"));
    stage.getPropertyValue(ScheduleGroupProperty.class)
      .orElseThrow(() -> new RuntimeException("ScheduleGroup property must be set for Stage"));

    stage.getIRDAG().getVertices().forEach(irVertex -> {
      // Check vertex type.
      if (!(irVertex instanceof SourceVertex
        || irVertex instanceof OperatorVertex)) {
        throw new UnsupportedOperationException(irVertex.toString());
      }
    });
  }
}

