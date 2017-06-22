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
package edu.snu.vortex.runtime.common.plan.physical;


import edu.snu.vortex.compiler.ir.Reader;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.RuntimeAttributeMap;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.common.plan.logical.*;
import edu.snu.vortex.runtime.exception.IllegalVertexOperationException;
import edu.snu.vortex.runtime.exception.PhysicalPlanGenerationException;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.common.dag.DAGBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A function that converts a runtime's logical DAG to physical DAG.
 */
public final class PhysicalDAGGenerator
    implements Function<DAG<Stage, StageEdge>, DAG<PhysicalStage, PhysicalStageEdge>> {

  private final DAGBuilder<PhysicalStage, PhysicalStageEdge> physicalDAGBuilder;

  public PhysicalDAGGenerator() {
    physicalDAGBuilder = new DAGBuilder<>();
  }

  /**
   * Converts the given logical DAG to a physical DAG for execution.
   * @param logicalDAG submitted to Runtime in {@link ExecutionPlan}, an output of {@link LogicalDAGGenerator}.
   * @return the converted physical DAG to execute,
   * which consists of {@link PhysicalStage} and their relationship represented by {@link PhysicalStageEdge}.
   */
  @Override
  public DAG<PhysicalStage, PhysicalStageEdge> apply(final DAG<Stage, StageEdge> logicalDAG) {
    final Map<String, PhysicalStage> runtimeStageIdToPhysicalStageMap = new HashMap<>();

    try {
      final Map<String, Task> runtimeVertexIdToTask = new HashMap<>();
      PhysicalStageBuilder physicalStageBuilder;
      for (final Stage stage : logicalDAG.getVertices()) {

        final List<RuntimeVertex> stageVertices = stage.getStageInternalDAG().getVertices();

        final RuntimeAttributeMap firstVertexAttrs = stageVertices.iterator().next().getVertexAttributes();
        int stageParallelism = firstVertexAttrs.get(RuntimeAttribute.IntegerKey.Parallelism);
        stageVertices.forEach(runtimeVertex -> {
          // This check should be done in the compiler backend
          int vertexParallelism =
              runtimeVertex.getVertexAttributes().get(RuntimeAttribute.IntegerKey.Parallelism);
          if (vertexParallelism != stageParallelism) {
            // TODO #103: Integrity check in execution plan.
            throw new RuntimeException("All vertices in a stage should have same parallelism");
          }
        });
        final RuntimeAttribute containerType = firstVertexAttrs.get(RuntimeAttribute.Key.ContainerType);

        // Begin building a new stage in the physical plan.
        physicalStageBuilder = new PhysicalStageBuilder(stage.getStageId(), stageParallelism);

        // (parallelism) number of task groups will be created.
        for (int taskGroupIdx = 0; taskGroupIdx < stageParallelism; taskGroupIdx++) {
          final DAGBuilder<Task, RuntimeEdge<Task>> stageInternalDAGBuilder = new DAGBuilder<>();
          Task newTaskToAdd;

          // Iterate over the vertices contained in this stage to convert to tasks.
          for (final RuntimeVertex vertex : stageVertices) {
            if (vertex instanceof RuntimeBoundedSourceVertex) {
              final RuntimeBoundedSourceVertex boundedSourceVertex = (RuntimeBoundedSourceVertex) vertex;

              final List<Reader> readers =
                  boundedSourceVertex.getBoundedSourceVertex().getReaders(stageParallelism);
              if (readers.size() != stageParallelism) {
                throw new RuntimeException("Actual parallelism differs from the one specified by IR: "
                    + readers.size() + " and " + stageParallelism);
              }
              newTaskToAdd = new BoundedSourceTask(RuntimeIdGenerator.generateTaskId(),
                  boundedSourceVertex.getId(), taskGroupIdx, readers.get(taskGroupIdx));
            } else if (vertex instanceof RuntimeOperatorVertex) {
              final RuntimeOperatorVertex operatorVertex = (RuntimeOperatorVertex) vertex;
              newTaskToAdd = new OperatorTask(RuntimeIdGenerator.generateTaskId(), operatorVertex.getId(),
                  taskGroupIdx, operatorVertex.getOperatorVertex().getTransform());
            } else {
              throw new IllegalVertexOperationException("This vertex type is not supported");
            }
            stageInternalDAGBuilder.addVertex(newTaskToAdd);
            runtimeVertexIdToTask.put(vertex.getId(), newTaskToAdd);
          }

          // Now that all tasks have been created, connect the internal edges in the task group.
          // Notice that it suffices to iterate over only the internalInEdges.
          final DAG<RuntimeVertex, RuntimeEdge<RuntimeVertex>> internalInEdges = stage.getStageInternalDAG();
          internalInEdges.getVertices().forEach(runtimeVertex -> {
            final List<RuntimeEdge<RuntimeVertex>> inEdges = internalInEdges.getIncomingEdgesOf(runtimeVertex);
            inEdges.forEach(edge -> stageInternalDAGBuilder
                .connectVertices(new RuntimeEdge<>(edge.getId(), edge.getEdgeAttributes(),
                    runtimeVertexIdToTask.get(edge.getSrc().getId()),
                    runtimeVertexIdToTask.get(edge.getDst().getId()), edge.getCoder())));

          });

          // Create the task group to add for this stage.
          final TaskGroup newTaskGroup =
              new TaskGroup(RuntimeIdGenerator.generateTaskGroupId(), stage.getId(), taskGroupIdx,
                  stageInternalDAGBuilder.build(), containerType);
          physicalStageBuilder.addTaskGroup(newTaskGroup);
          runtimeVertexIdToTask.clear();
        }
        final PhysicalStage physicalStage = physicalStageBuilder.build();
        physicalDAGBuilder.addVertex(physicalStage);
        runtimeStageIdToPhysicalStageMap.put(stage.getStageId(), physicalStage);
      }

      // Connect PhysicalStage
      for (final Stage stage : logicalDAG.getVertices()) {
        logicalDAG.getIncomingEdgesOf(stage).forEach(stageEdge -> {
          final PhysicalStage srcStage = runtimeStageIdToPhysicalStageMap.get(stageEdge.getSrc().getStageId());
          final PhysicalStage dstStage = runtimeStageIdToPhysicalStageMap.get(stageEdge.getDst().getStageId());

          physicalDAGBuilder.connectVertices(new PhysicalStageEdge(stageEdge.getId(),
              stageEdge.getEdgeAttributes(),
              stageEdge.getSrcRuntimeVertex(), stageEdge.getDstRuntimeVertex(),
              stageEdge.getSrcRuntimeVertex().getVertexAttributes(), srcStage, dstStage, stageEdge.getCoder()));

        });
      }
    } catch (final Exception e) {
      throw new PhysicalPlanGenerationException(e);
    }
    return physicalDAGBuilder.build();
  }
}
