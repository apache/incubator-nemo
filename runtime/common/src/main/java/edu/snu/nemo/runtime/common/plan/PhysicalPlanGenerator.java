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
import edu.snu.nemo.common.ir.vertex.*;
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ScheduleGroupIndexProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.StageIdProperty;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.exception.IllegalVertexOperationException;
import edu.snu.nemo.common.exception.PhysicalPlanGenerationException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.function.Function;

/**
 * A function that converts an IR DAG to physical DAG.
 */
public final class PhysicalPlanGenerator implements Function<DAG<IRVertex, IREdge>, DAG<Stage, StageEdge>> {
  private final Map<String, IRVertex> idToIRVertex;
  private final String dagDirectory;

  /**
   * Private constructor.
   *
   * @param dagDirectory the directory in which to store DAG data.
   */
  @Inject
  private PhysicalPlanGenerator(@Parameter(JobConf.DAGDirectory.class) final String dagDirectory) {
    this.idToIRVertex = new HashMap<>();
    this.dagDirectory = dagDirectory;
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

    // this is needed because of DuplicateEdgeGroupProperty.
    handleDuplicateEdgeGroupProperty(dagOfStages);

    // for debugging purposes.
    dagOfStages.storeJSON(dagDirectory, "plan-logical", "logical execution plan");

    return dagOfStages;
  }

  public Map<String, IRVertex> getIdToIRVertex() {
    return idToIRVertex;
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

    final Map<Integer, List<IRVertex>> vertexListForEachStage = new LinkedHashMap<>();
    irDAG.topologicalDo(irVertex -> {
      final Integer stageNum = irVertex.getPropertyValue(StageIdProperty.class).get();
      if (!vertexListForEachStage.containsKey(stageNum)) {
        vertexListForEachStage.put(stageNum, new ArrayList<>());
      }
      vertexListForEachStage.get(stageNum).add(irVertex);
    });

    final Map<IRVertex, Stage> vertexStageMap = new HashMap<>();

    for (final List<IRVertex> stageVertices : vertexListForEachStage.values()) {
      integrityCheck(stageVertices);

      final Set<IRVertex> currentStageVertices = new HashSet<>();
      final Set<StageEdgeBuilder> currentStageIncomingEdges = new HashSet<>();

      // Create a new stage builder.
      final IRVertex irVertexOfNewStage = stageVertices.stream().findAny()
          .orElseThrow(() -> new RuntimeException("Error: List " + stageVertices.getClass() + " is Empty"));
      final StageBuilder stageBuilder = new StageBuilder(
          irVertexOfNewStage.getPropertyValue(StageIdProperty.class).get(),
          irVertexOfNewStage.getPropertyValue(ParallelismProperty.class).get(),
          irVertexOfNewStage.getPropertyValue(ScheduleGroupIndexProperty.class).get(),
          irVertexOfNewStage.getPropertyValue(ExecutorPlacementProperty.class).get());

      // Prepare useful variables.
      final int stageParallelism = irVertexOfNewStage.getPropertyValue(ParallelismProperty.class).get();
      final List<Map<String, Readable>> vertexIdToReadables = new ArrayList<>(stageParallelism);
      for (int i = 0; i < stageParallelism; i++) {
        vertexIdToReadables.add(new HashMap<>());
      }

      // For each vertex in the stage,
      for (final IRVertex irVertex : stageVertices) {
        // Take care of the readables of a source vertex.
        if (irVertex instanceof SourceVertex) {
          final SourceVertex sourceVertex = (SourceVertex) irVertex;
          try {
            final List<Readable> readables = sourceVertex.getReadables(stageParallelism);
            for (int i = 0; i < stageParallelism; i++) {
              vertexIdToReadables.get(i).put(irVertex.getId(), readables.get(i));
            }
          } catch (Exception e) {
            throw new PhysicalPlanGenerationException(e);
          }

          // Clear internal metadata.
          sourceVertex.clearInternalStates();
        }

        // Add vertex to the stage.
        stageBuilder.addVertex(irVertex);
        currentStageVertices.add(irVertex);

        // Connect all the incoming edges for the vertex.
        final List<IREdge> inEdges = irDAG.getIncomingEdgesOf(irVertex);
        final Optional<List<IREdge>> inEdgeList = (inEdges == null) ? Optional.empty() : Optional.of(inEdges);
        inEdgeList.ifPresent(edges -> edges.forEach(irEdge -> {
          final IRVertex srcVertex = irEdge.getSrc();
          final IRVertex dstVertex = irEdge.getDst();

          if (srcVertex == null) {
            throw new IllegalVertexOperationException("Unable to locate srcVertex for IREdge " + irEdge);
          } else if (dstVertex == null) {
            throw new IllegalVertexOperationException("Unable to locate dstVertex for IREdge " + irEdge);
          }

          // both vertices are in the stage.
          if (currentStageVertices.contains(srcVertex) && currentStageVertices.contains(dstVertex)) {
            stageBuilder.connectInternalVertices(new RuntimeEdge<IRVertex>(
                irEdge.getId(),
                irEdge.getExecutionProperties(),
                irEdge.getSrc(),
                irEdge.getDst(),
                irEdge.isSideInput()));
          } else { // edge comes from another stage
            final Stage srcStage = vertexStageMap.get(srcVertex);

            if (srcStage == null) {
              throw new IllegalVertexOperationException("srcVertex " + srcVertex.getId()
                  + " not yet added to the builder");
            }

            final StageEdgeBuilder newEdgeBuilder = new StageEdgeBuilder(irEdge.getId())
                .setEdgeProperties(irEdge.getExecutionProperties())
                .setSrcVertex(srcVertex)
                .setDstVertex(dstVertex)
                .setSrcStage(srcStage)
                .setSideInputFlag(irEdge.isSideInput());
            currentStageIncomingEdges.add(newEdgeBuilder);
          }
        }));

        // Track id to irVertex.
        idToIRVertex.put(irVertex.getId(), irVertex);
      }
      stageBuilder.addReadables(vertexIdToReadables);
      // If this runtime stage contains at least one vertex, build it!
      if (!stageBuilder.isEmpty()) {
        final Stage currentStage = stageBuilder.build();
        dagOfStagesBuilder.addVertex(currentStage);

        // Add this stage as the destination stage for all the incoming edges.
        currentStageIncomingEdges.forEach(stageEdgeBuilder -> {
          stageEdgeBuilder.setDstStage(currentStage);
          final StageEdge stageEdge = stageEdgeBuilder.build();
          dagOfStagesBuilder.connectVertices(stageEdge);
        });
        currentStageIncomingEdges.clear();

        currentStageVertices.forEach(irVertex -> vertexStageMap.put(irVertex, currentStage));
        currentStageVertices.clear();
      }
    }

    return dagOfStagesBuilder.build();
  }

  /**
   * Integrity check for a stage's vertices.
   * @param stageVertices to check for
   */
  private void integrityCheck(final List<IRVertex> stageVertices) {
    final IRVertex firstVertex = stageVertices.get(0);
    final String placement = firstVertex.getPropertyValue(ExecutorPlacementProperty.class).get();
    final int scheduleGroup = firstVertex.getPropertyValue(ScheduleGroupIndexProperty.class).get();
    final int parallelism = firstVertex.getPropertyValue(ParallelismProperty.class).get();

    stageVertices.forEach(irVertex -> {
      // Check vertex type.
      if (!(irVertex instanceof  SourceVertex
          || irVertex instanceof OperatorVertex
          || irVertex instanceof MetricCollectionBarrierVertex)) {
        throw new UnsupportedOperationException(irVertex.toString());
      }

      // Check execution properties.
      if ((placement != null
          && !placement.equals(irVertex.getPropertyValue(ExecutorPlacementProperty.class).get()))
          || scheduleGroup != irVertex.getPropertyValue(ScheduleGroupIndexProperty.class).get()
          || parallelism != irVertex.getPropertyValue(ParallelismProperty.class).get()) {
        throw new RuntimeException("Vertices of the same stage have different execution properties: "
            + irVertex.getId());
      }
    });
  }
}
