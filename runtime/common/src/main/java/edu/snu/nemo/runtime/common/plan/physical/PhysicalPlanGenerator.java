/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.nemo.runtime.common.plan.physical;

import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.common.ir.vertex.*;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;
import edu.snu.nemo.runtime.common.plan.stage.*;
import edu.snu.nemo.common.exception.IllegalVertexOperationException;
import edu.snu.nemo.common.exception.PhysicalPlanGenerationException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.function.Function;

/**
 * A function that converts an IR DAG to physical DAG.
 */
public final class PhysicalPlanGenerator
    implements Function<DAG<IRVertex, IREdge>, DAG<PhysicalStage, PhysicalStageEdge>> {

  final String dagDirectory;

  /**
   * Private constructor.
   *
   * @param dagDirectory the directory in which to store DAG data.
   */
  @Inject
  private PhysicalPlanGenerator(@Parameter(JobConf.DAGDirectory.class) final String dagDirectory) {
    this.dagDirectory = dagDirectory;
  }

  /**
   * Generates the {@link PhysicalPlan} to be executed.
   *
   * @param irDAG that should be converted to a physical execution plan
   * @return {@link PhysicalPlan} to execute.
   */
  @Override
  public DAG<PhysicalStage, PhysicalStageEdge> apply(final DAG<IRVertex, IREdge> irDAG) {
    // first, stage-partition the IR DAG.
    final DAG<Stage, StageEdge> dagOfStages = stagePartitionIrDAG(irDAG);
    // for debugging purposes.
    dagOfStages.storeJSON(dagDirectory, "plan-logical", "logical execution plan");
    // then create tasks and make it into a physical execution plan.
    return stagesIntoPlan(dagOfStages);
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
      final Integer stageNum = irVertex.getProperty(ExecutionProperty.Key.StageId);
      if (!vertexListForEachStage.containsKey(stageNum)) {
        vertexListForEachStage.put(stageNum, new ArrayList<>());
      }
      vertexListForEachStage.get(stageNum).add(irVertex);
    });

    final Map<IRVertex, Stage> vertexStageMap = new HashMap<>();

    for (final List<IRVertex> stageVertices : vertexListForEachStage.values()) {
      final Set<IRVertex> currentStageVertices = new HashSet<>();
      final Set<StageEdgeBuilder> currentStageIncomingEdges = new HashSet<>();

      // Create a new stage builder.
      final IRVertex irVertexOfNewStage = stageVertices.stream().findAny()
          .orElseThrow(() -> new RuntimeException("Error: List " + stageVertices.getClass() + " is Empty"));
      final StageBuilder stageBuilder =
          new StageBuilder(irVertexOfNewStage.getProperty(ExecutionProperty.Key.StageId),
              irVertexOfNewStage.getProperty(ExecutionProperty.Key.ScheduleGroupIndex));

      // For each vertex in the stage,
      for (final IRVertex irVertex : stageVertices) {

        // Add vertex to the stage.
        stageBuilder.addVertex(irVertex);
        currentStageVertices.add(irVertex);

        // Connect all the incoming edges for the vertex
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
            stageBuilder.connectInternalVertices(irEdge);
          } else { // edge comes from another stage
            final Stage srcStage = vertexStageMap.get(srcVertex);

            if (srcStage == null) {
              throw new IllegalVertexOperationException("srcVertex " + srcVertex.getId()
                  + " not yet added to the builder");
            }

            final StageEdgeBuilder newEdgeBuilder = new StageEdgeBuilder(irEdge.getId())
                .setEdgeProperties(irEdge.getExecutionProperties())
                .setSrcVertex(srcVertex).setDstVertex(dstVertex)
                .setSrcStage(srcStage)
                .setCoder(irEdge.getCoder())
                .setSideInputFlag(irEdge.isSideInput());
            currentStageIncomingEdges.add(newEdgeBuilder);
          }
        }));
      }

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

  // Map that keeps track of the IRVertex of each tasks
  private final Map<Task, IRVertex> taskIRVertexMap = new HashMap<>();

  /**
   * Getter for taskIRVertexMap.
   *
   * @return the taskIRVertexMap.
   */
  public Map<Task, IRVertex> getTaskIRVertexMap() {
    return taskIRVertexMap;
  }

  /**
   * Converts the given DAG of stages to a physical DAG for execution.
   *
   * @param dagOfStages IR DAG partitioned into stages.
   * @return the converted physical DAG to execute,
   * which consists of {@link PhysicalStage} and their relationship represented by {@link PhysicalStageEdge}.
   */
  private DAG<PhysicalStage, PhysicalStageEdge> stagesIntoPlan(final DAG<Stage, StageEdge> dagOfStages) {
    final Map<String, PhysicalStage> runtimeStageIdToPhysicalStageMap = new HashMap<>();
    final DAGBuilder<PhysicalStage, PhysicalStageEdge> physicalDAGBuilder = new DAGBuilder<>();

    for (final Stage stage : dagOfStages.getVertices()) {
      final Map<IRVertex, Task> irVertexTaskMap = new HashMap<>();
      final List<IRVertex> stageVertices = stage.getStageInternalDAG().getVertices();

      final ExecutionPropertyMap firstVertexProperties = stageVertices.iterator().next().getExecutionProperties();
      final int stageParallelism = firstVertexProperties.get(ExecutionProperty.Key.Parallelism);
      final String containerType = firstVertexProperties.get(ExecutionProperty.Key.ExecutorPlacement);

      // Only one task group DAG will be created and reused.
      final DAGBuilder<Task, RuntimeEdge<Task>> stageInternalDAGBuilder = new DAGBuilder<>();
      // Collect split source readables in advance and bind to each scheduled task group to avoid extra source split.
      final List<Map<String, Readable>> logicalTaskIdToReadables = new ArrayList<>(stageParallelism);
      for (int i = 0; i < stageParallelism; i++) {
        logicalTaskIdToReadables.add(new HashMap<>());
      }

      // Iterate over the vertices contained in this stage to convert to tasks.
      stageVertices.forEach(irVertex -> {
        final Task newTaskToAdd;
        if (irVertex instanceof SourceVertex) {
          final SourceVertex sourceVertex = (SourceVertex) irVertex;

          try {
            final List<Readable> readables = sourceVertex.getReadables(stageParallelism);
            final String irVertexId = sourceVertex.getId();
            final String logicalTaskId = RuntimeIdGenerator.generateLogicalTaskId(irVertexId);
            for (int i = 0; i < stageParallelism; i++) {
              logicalTaskIdToReadables.get(i).put(logicalTaskId, readables.get(i));
            }
            newTaskToAdd = new BoundedSourceTask(logicalTaskId, irVertexId);
          } catch (Exception e) {
            throw new PhysicalPlanGenerationException(e);
          }
        } else if (irVertex instanceof OperatorVertex) {
          final OperatorVertex operatorVertex = (OperatorVertex) irVertex;
          final String operatorVertexId = operatorVertex.getId();
          newTaskToAdd = new OperatorTask(RuntimeIdGenerator.generateLogicalTaskId(operatorVertexId),
              operatorVertexId, operatorVertex.getTransform());

        } else if (irVertex instanceof MetricCollectionBarrierVertex) {
          final MetricCollectionBarrierVertex metricCollectionBarrierVertex =
              (MetricCollectionBarrierVertex) irVertex;
          final String metricVertexId = metricCollectionBarrierVertex.getId();
          newTaskToAdd = new MetricCollectionBarrierTask(RuntimeIdGenerator.generateLogicalTaskId(metricVertexId),
              metricVertexId);
        } else {
          throw new IllegalVertexOperationException("This vertex type is not supported");
        }
        stageInternalDAGBuilder.addVertex(newTaskToAdd);
        irVertexTaskMap.put(irVertex, newTaskToAdd);
        taskIRVertexMap.put(newTaskToAdd, irVertex);
      });

      // connect internal edges in the task group. It suffices to iterate over only the stage internal inEdges.
      final DAG<IRVertex, IREdge> stageInternalDAG = stage.getStageInternalDAG();
      stageInternalDAG.getVertices().forEach(irVertex -> {
        final List<IREdge> inEdges = stageInternalDAG.getIncomingEdgesOf(irVertex);
        inEdges.forEach(edge ->
            stageInternalDAGBuilder.connectVertices(new RuntimeEdge<>(edge.getId(), edge.getExecutionProperties(),
                irVertexTaskMap.get(edge.getSrc()), irVertexTaskMap.get(edge.getDst()),
                edge.getCoder(), edge.isSideInput())));
      });

      // Create the task group to add for this stage.
      final PhysicalStage physicalStage =
          new PhysicalStage(stage.getId(), stageInternalDAGBuilder.build(),
              stageParallelism, stage.getScheduleGroupIndex(), containerType, logicalTaskIdToReadables);

      physicalDAGBuilder.addVertex(physicalStage);
      runtimeStageIdToPhysicalStageMap.put(stage.getId(), physicalStage);
    }

    // Connect Physical stages
    dagOfStages.getVertices().forEach(stage ->
        dagOfStages.getIncomingEdgesOf(stage).forEach(stageEdge -> {
          final PhysicalStage srcStage = runtimeStageIdToPhysicalStageMap.get(stageEdge.getSrc().getId());
          final PhysicalStage dstStage = runtimeStageIdToPhysicalStageMap.get(stageEdge.getDst().getId());

          physicalDAGBuilder.connectVertices(new PhysicalStageEdge(stageEdge.getId(),
              stageEdge.getExecutionProperties(),
              stageEdge.getSrcVertex(), stageEdge.getDstVertex(),
              srcStage, dstStage,
              stageEdge.getCoder(),
              stageEdge.isSideInput()));
        }));

    return physicalDAGBuilder.build();
  }
}
