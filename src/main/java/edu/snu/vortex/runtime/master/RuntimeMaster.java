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
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.compiler.ir.Reader;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.RuntimeAttributeMap;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.plan.logical.*;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.runtime.exception.IllegalVertexOperationException;
import edu.snu.vortex.runtime.exception.PhysicalPlanGenerationException;
import edu.snu.vortex.utils.DAG;
import edu.snu.vortex.utils.DAGImpl;

import java.util.*;
import java.util.logging.Logger;

/**
 * Runtime Master is the central controller of Runtime.
 * Compiler submits an {@link ExecutionPlan} to Runtime Master to execute a job.
 * Runtime Master handles:
 *    a) Physical conversion of a job's DAG into a physical plan.
 *    b) Scheduling the job with {@link edu.snu.vortex.runtime.master.scheduler.Scheduler}.
 *    c) (Please list others done by Runtime Master as features are added).
 */
public final class RuntimeMaster {
  private static final Logger LOG = Logger.getLogger(RuntimeMaster.class.getName());
  // TODO #93: Implement Batch Scheduler
  // private final Scheduler scheduler;

  public RuntimeMaster() {
    // TODO #93: Implement Batch Scheduler
    // this.scheduler = new Scheduler(RuntimeAttribute.Batch);
  }

  /**
   * Submits the {@link ExecutionPlan} to Runtime.
   * @param executionPlan to execute.
   */
  public void execute(final ExecutionPlan executionPlan) {
    final PhysicalPlan physicalPlan = generatePhysicalPlan(executionPlan);
    // TODO #93: Implement Batch Scheduler
    // scheduler.scheduleJob(physicalPlan);
    try {
      new SimpleRuntime().executePhysicalPlan(physicalPlan);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Generates the {@link PhysicalPlan} to be executed.
   * @param executionPlan that should be converted to a physical plan
   * @return {@link PhysicalPlan} to execute.
   */
  private PhysicalPlan generatePhysicalPlan(final ExecutionPlan executionPlan) {
    final PhysicalPlanBuilder physicalPlanBuilder = new PhysicalPlanBuilder(executionPlan.getId());

    try {
      final Map<String, Task> runtimeVertexIdToTask = new HashMap<>();

      for (final RuntimeStage runtimeStage : executionPlan.getRuntimeStages()) {
        final List<RuntimeVertex> stageVertices = runtimeStage.getRuntimeVertices();

        final Map<String, Set<StageBoundaryEdgeInfo>> incomingEdgeInfos =
            createStageBoundaryEdgeInfo(runtimeStage.getStageIncomingEdges(), true);
        final Map<String, Set<StageBoundaryEdgeInfo>> outgoingEdgeInfos =
            createStageBoundaryEdgeInfo(runtimeStage.getStageOutgoingEdges(), false);

        final RuntimeAttributeMap firstVertexAttrs = stageVertices.get(0).getVertexAttributes();
        final Integer stageParallelism = firstVertexAttrs.get(RuntimeAttribute.IntegerKey.Parallelism);
        stageVertices.forEach(runtimeVertex -> {
          if (!stageParallelism.equals(runtimeVertex
              .getVertexAttributes().get(RuntimeAttribute.IntegerKey.Parallelism))) {
            // This check should be done in the compiler backend
            // TODO #103: Integrity check in execution plan.
            throw new RuntimeException("All vertices in a stage should have same parallelism");
          }
        });


        final RuntimeAttribute resourceType = firstVertexAttrs.get(RuntimeAttribute.Key.ResourceType);

        // Begin building a new stage in the physical plan.
        physicalPlanBuilder.createNewStage(stageParallelism);

        // (parallelism) number of task groups will be created.
        for (int taskGroupIdx = 0; taskGroupIdx < stageParallelism; taskGroupIdx++) {
          final DAG<Task> taskDAG = new DAGImpl<>();
          Task newTaskToAdd;

          // Iterate over the vertices contained in this stage to convert to tasks.
          for (final RuntimeVertex vertex : stageVertices) {
            if (vertex instanceof RuntimeBoundedSourceVertex) {
              final RuntimeBoundedSourceVertex boundedSourceVertex = (RuntimeBoundedSourceVertex) vertex;

              final List<Reader> readers = boundedSourceVertex.getBoundedSourceVertex().getReaders(stageParallelism);
              if (readers.size() != stageParallelism) {
                throw new RuntimeException("Actual parallelism differs from the one specified by IR");
              }
              newTaskToAdd = new BoundedSourceTask(RuntimeIdGenerator.generateTaskId(),
                  boundedSourceVertex.getId(), taskGroupIdx, readers.get(taskGroupIdx));
            } else if (vertex instanceof RuntimeOperatorVertex) {
              final RuntimeOperatorVertex operatorVertex = (RuntimeOperatorVertex) vertex;
              newTaskToAdd = new OperatorTask(RuntimeIdGenerator.generateTaskId(), operatorVertex.getId(), taskGroupIdx,
                  operatorVertex.getOperatorVertex().getTransform());
            } else {
              throw new IllegalVertexOperationException("This vertex type is not supported");
            }
            taskDAG.addVertex(newTaskToAdd);
            runtimeVertexIdToTask.put(vertex.getId(), newTaskToAdd);
          }

          // Now that all tasks have been created, connect the internal edges in the task group.
          // Notice that it suffices to iterate over only the internalInEdges.
          final Map<String, Set<String>> internalInEdges = runtimeStage.getInternalInEdges();
          for (final Map.Entry<String, Set<String>> entry : internalInEdges.entrySet()) {
            final Set<String> dstVertexIds = entry.getValue();
            dstVertexIds.forEach(id -> {
              taskDAG.addEdge(runtimeVertexIdToTask.get(id), runtimeVertexIdToTask.get(entry.getKey()));
            });
          }

          // Create the task group to add for this stage.
          final TaskGroup newTaskGroup =
              new TaskGroup(RuntimeIdGenerator.generateTaskGroupId(), taskDAG, resourceType,
                  incomingEdgeInfos, outgoingEdgeInfos);
          physicalPlanBuilder.addTaskGroupToCurrentStage(newTaskGroup);
          runtimeVertexIdToTask.clear();
        }
      }
    } catch (final Exception e) {
      throw new PhysicalPlanGenerationException(e.getMessage());
    }
    final PhysicalPlan physicalPlan = physicalPlanBuilder.build();
    return physicalPlan;
  }

  /**
   * Creates a list of information on stage boundary edges for task groups.
   * @param stageBoundaryRuntimeEdges a map of this stage's vertex id to the incoming/outgoing edges from/to this stage.
   * @param isIncomingEdges true if the map is for incoming, false if it is for outgoing edges.
   * @return a map of information on the stage boundary edges.
   */
  private Map<String, Set<StageBoundaryEdgeInfo>> createStageBoundaryEdgeInfo(
      final Map<String, Set<RuntimeEdge>> stageBoundaryRuntimeEdges, final boolean isIncomingEdges) {
    final Map<String, Set<StageBoundaryEdgeInfo>> boundaryEdgeMap = new HashMap<>();

    for (final Map.Entry<String, Set<RuntimeEdge>> entry : stageBoundaryRuntimeEdges.entrySet()) {
      final Set<StageBoundaryEdgeInfo> stageBoundaryEdgeInfos = new HashSet<>();
      entry.getValue().forEach(runtimeEdge -> {
        final RuntimeVertex endpointVertex;
        if (isIncomingEdges) {
          endpointVertex = runtimeEdge.getSrcRuntimeVertex();
        } else {
          endpointVertex = runtimeEdge.getDstRuntimeVertex();
        }
        stageBoundaryEdgeInfos.add(new StageBoundaryEdgeInfo(runtimeEdge.getId(), runtimeEdge.getEdgeAttributes(),
            endpointVertex, endpointVertex.getVertexAttributes()));
      });
      boundaryEdgeMap.put(entry.getKey(), stageBoundaryEdgeInfos);
    }
    return boundaryEdgeMap;
  }
}
