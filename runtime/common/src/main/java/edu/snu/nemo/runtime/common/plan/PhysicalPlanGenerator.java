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
package edu.snu.nemo.runtime.common.plan;

import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupPropertyValue;
import edu.snu.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import edu.snu.nemo.common.ir.executionproperty.VertexExecutionProperty;
import edu.snu.nemo.common.ir.vertex.*;
import edu.snu.nemo.common.ir.vertex.executionproperty.DynamicOptimizationProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ScheduleGroupIndexProperty;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.exception.IllegalVertexOperationException;
import edu.snu.nemo.common.exception.PhysicalPlanGenerationException;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.function.Function;

/**
 * A function that converts an IR DAG to physical DAG.
 */
public final class PhysicalPlanGenerator implements Function<DAG<IRVertex, IREdge>, DAG<Stage, StageEdge>> {
  private final String dagDirectory;
  private final StagePartitioner stagePartitioner;

  /**
   * Private constructor.
   *
   * @param stagePartitioner provides stage partitioning
   * @param dagDirectory the directory in which to store DAG data.
   */
  @Inject
  private PhysicalPlanGenerator(final StagePartitioner stagePartitioner,
                                @Parameter(JobConf.DAGDirectory.class) final String dagDirectory) {
    this.dagDirectory = dagDirectory;
    this.stagePartitioner = stagePartitioner;
    stagePartitioner.addIgnoredPropertyKey(DynamicOptimizationProperty.class);
  }

  /**
   * Generates the {@link PhysicalPlan} to be executed.
   *
   * @param irDAG that should be converted to a physical execution plan
   * @return {@link PhysicalPlan} to execute.
   */
  @Override
  public DAG<Stage, StageEdge> apply(final DAG<IRVertex, IREdge> irDAG) {
    // first, stage-partition the IR DAG.
    final DAG<Stage, StageEdge> dagOfStages = stagePartitionIrDAG(irDAG);

    // Sanity check
    dagOfStages.getVertices().forEach(this::integrityCheck);

    // this is needed because of DuplicateEdgeGroupProperty.
    handleDuplicateEdgeGroupProperty(dagOfStages);

    // for debugging purposes.
    dagOfStages.storeJSON(dagDirectory, "plan-logical", "logical execution plan");

    return dagOfStages;
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
      final StageEdge representativeEdge = edges.get(0);
      final DuplicateEdgeGroupPropertyValue representativeProperty =
          representativeEdge.getPropertyValue(DuplicateEdgeGroupProperty.class).get();
      edges.forEach(e -> {
        final DuplicateEdgeGroupPropertyValue duplicateEdgeGroupProperty =
            e.getPropertyValue(DuplicateEdgeGroupProperty.class).get();
        duplicateEdgeGroupProperty.setRepresentativeEdgeId(representativeEdge.getId());
        duplicateEdgeGroupProperty.setGroupSize(representativeProperty.getGroupSize());
      });
    });
  }

  /**
   * We take the stage-partitioned DAG and create actual stage and stage edge objects to create a DAG of stages.
   *
   * @param irDAG stage-partitioned IR DAG.
   * @return the DAG composed of stages and stage edges.
   */
  public DAG<Stage, StageEdge> stagePartitionIrDAG(final DAG<IRVertex, IREdge> irDAG) {
    final DAGBuilder<Stage, StageEdge> dagOfStagesBuilder = new DAGBuilder<>();
    final Set<IREdge> interStageEdges = new HashSet<>();
    final Map<Integer, Stage> stageIdToStageMap = new HashMap<>();
    final Map<IRVertex, Integer> vertexToStageIdMap = stagePartitioner.apply(irDAG);

    final Map<Integer, Set<IRVertex>> vertexSetForEachStage = new LinkedHashMap<>();
    irDAG.topologicalDo(irVertex -> {
      final int stageId = vertexToStageIdMap.get(irVertex);
      if (!vertexSetForEachStage.containsKey(stageId)) {
        vertexSetForEachStage.put(stageId, new HashSet<>());
      }
      vertexSetForEachStage.get(stageId).add(irVertex);
    });

    for (final int stageId : vertexSetForEachStage.keySet()) {
      final Set<IRVertex> stageVertices = vertexSetForEachStage.get(stageId);
      final String stageIdentifier = RuntimeIdGenerator.generateStageId(stageId);
      final ExecutionPropertyMap<VertexExecutionProperty> stageProperties = new ExecutionPropertyMap<>(stageIdentifier);
      stagePartitioner.getStageProperties(stageVertices.iterator().next()).forEach(stageProperties::put);
      final int stageParallelism = stageProperties.get(ParallelismProperty.class)
          .orElseThrow(() -> new RuntimeException("Parallelism property must be set for Stage"));

      final DAGBuilder<IRVertex, RuntimeEdge<IRVertex>> stageInternalDAGBuilder = new DAGBuilder<>();

      // Prepare vertexIdtoReadables
      final List<Map<String, Readable>> vertexIdToReadables = new ArrayList<>(stageParallelism);
      for (int i = 0; i < stageParallelism; i++) {
        vertexIdToReadables.add(new HashMap<>());
      }

      // For each IRVertex,
      for (final IRVertex irVertex : stageVertices) {
        // Take care of the readables of a source vertex.
        if (irVertex instanceof SourceVertex) {
          final SourceVertex sourceVertex = (SourceVertex) irVertex;
          try {
            final List<Readable> readables = sourceVertex.getReadables(stageParallelism);
            for (int i = 0; i < stageParallelism; i++) {
              vertexIdToReadables.get(i).put(irVertex.getId(), readables.get(i));
            }
          } catch (final Exception e) {
            throw new PhysicalPlanGenerationException(e);
          }
          // Clear internal metadata.
          sourceVertex.clearInternalStates();
        }

        // Add vertex to the stage.
        stageInternalDAGBuilder.addVertex(irVertex);
      }

      for (final IRVertex dstVertex : stageVertices) {
        // Connect all the incoming edges for the vertex.
        irDAG.getIncomingEdgesOf(dstVertex).forEach(irEdge -> {
          final IRVertex srcVertex = irEdge.getSrc();

          // both vertices are in the same stage.
          if (vertexToStageIdMap.get(srcVertex).equals(vertexToStageIdMap.get(dstVertex))) {
            stageInternalDAGBuilder.connectVertices(new RuntimeEdge<>(
                irEdge.getId(),
                irEdge.getExecutionProperties(),
                irEdge.getSrc(),
                irEdge.getDst(),
                irEdge.isSideInput()));
          } else { // edge comes from another stage
            interStageEdges.add(irEdge);
          }
        });
      }
      // If this runtime stage contains at least one vertex, build it!
      if (!stageInternalDAGBuilder.isEmpty()) {
        final DAG<IRVertex, RuntimeEdge<IRVertex>> stageInternalDAG
            = stageInternalDAGBuilder.buildWithoutSourceSinkCheck();
        final Stage stage = new Stage(stageIdentifier, stageInternalDAG, stageProperties, vertexIdToReadables);
        dagOfStagesBuilder.addVertex(stage);
        stageIdToStageMap.put(stageId, stage);
      }
    }

    // Add StageEdges
    for (final IREdge interStageEdge : interStageEdges) {
      final Stage srcStage = stageIdToStageMap.get(vertexToStageIdMap.get(interStageEdge.getSrc()));
      final Stage dstStage = stageIdToStageMap.get(vertexToStageIdMap.get(interStageEdge.getDst()));
      if (srcStage == null || dstStage == null) {
        throw new IllegalVertexOperationException(String.format("Stage not added to the builder:%s%s",
            srcStage == null ? String.format(" source stage for %s", interStageEdge.getSrc()) : "",
            dstStage == null ? String.format(" destination stage for %s", interStageEdge.getDst()) : ""));
      }
      dagOfStagesBuilder.connectVertices(new StageEdge(interStageEdge.getId(), interStageEdge.getExecutionProperties(),
          interStageEdge.getSrc(), interStageEdge.getDst(), srcStage, dstStage, interStageEdge.isSideInput()));
    }

    return dagOfStagesBuilder.build();
  }

  /**
   * Integrity check for Stage.
   * @param stage to check for
   */
  private void integrityCheck(final Stage stage) {
    stage.getPropertyValue(ParallelismProperty.class)
        .orElseThrow(() -> new RuntimeException("Parallelism property must be set for Stage"));
    stage.getPropertyValue(ScheduleGroupIndexProperty.class)
        .orElseThrow(() -> new RuntimeException("ScheduleGroupIndex property must be set for Stage"));

    stage.getIRDAG().getVertices().forEach(irVertex -> {
      // Check vertex type.
      if (!(irVertex instanceof  SourceVertex
          || irVertex instanceof OperatorVertex
          || irVertex instanceof MetricCollectionBarrierVertex)) {
        throw new UnsupportedOperationException(irVertex.toString());
      }
    });
  }
}
