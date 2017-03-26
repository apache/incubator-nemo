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
 * Runtime Master.
 */
public final class RuntimeMaster {
  private static final Logger LOG = Logger.getLogger(RuntimeMaster.class.getName());

  /**
   * Submits the {@link ExecutionPlan} to Runtime.
   * @param executionPlan to execute.
   */
  public void execute(final ExecutionPlan executionPlan) {
    generatePhysicalPlan(executionPlan);
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
        final List<RuntimeVertex> runtimeVertices = runtimeStage.getRuntimeVertices();

        final List<StageBoundaryEdgeInfo> incomingEdgeInfos =
            createStageBoundaryEdgeInfo(runtimeStage.getStageIncomingEdges(), true);
        final List<StageBoundaryEdgeInfo> outgoingEdgeInfos =
            createStageBoundaryEdgeInfo(runtimeStage.getStageOutgoingEdges(), false);

        // TODO #103: Integrity check in execution plan.
        // This code simply assumes that all vertices follow the first vertex's parallelism.
        final int parallelism = (int) runtimeVertices.get(0).getVertexAttributes()
            .get(RuntimeAttribute.IntegerKey.Parallelism);

        // Begin building a new stage in the physical plan.
        physicalPlanBuilder.createNewStage(runtimeStage.getStageId(), parallelism);

        // (parallelism) number of task groups will be created.
        for (int taskGroupIdx = 0; taskGroupIdx < parallelism; taskGroupIdx++) {
          final DAG<Task> taskDAG = new DAGImpl<>();
          Task newTaskToAdd;

          // Iterate over the vertices contained in this stage to convert to tasks.
          for (final RuntimeVertex vertex : runtimeVertices) {
            if (vertex instanceof RuntimeBoundedSourceVertex) {
              final RuntimeBoundedSourceVertex boundedSourceVertex = (RuntimeBoundedSourceVertex) vertex;

              // TODO #104: Change the interface of getReaders() in SourceVertex.
              // This code assumes that the issue #104 has been resolved.
              final List<Reader> readers = boundedSourceVertex.getBoundedSourceVertex().getReaders(parallelism);
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
              new TaskGroup(RuntimeIdGenerator.generateTaskGroupId(), taskDAG, incomingEdgeInfos, outgoingEdgeInfos);
          physicalPlanBuilder.addTaskGroupToStage(runtimeStage.getStageId(), newTaskGroup);
          runtimeVertexIdToTask.clear();
        }
      }
    } catch (final Exception e) {
      throw new PhysicalPlanGenerationException(e.getMessage());
    }
    return physicalPlanBuilder.build();
  }

  /**
   * Creates a list of information on stage boundary edges for task groups.
   * @param stageBoundaryRuntimeEdges a map of endpoint vertex id to the incoming/outgoing edges from/to this stage.
   * @param isIncomingEdges true if the map is for incoming, false if it is for outgoing edges.
   * @return a list of information on the stage boundary edges.
   */
  private List<StageBoundaryEdgeInfo> createStageBoundaryEdgeInfo(
      final Map<String, Set<RuntimeEdge>> stageBoundaryRuntimeEdges, final boolean isIncomingEdges) {
    final List<StageBoundaryEdgeInfo> stageBoundaryEdgeInfos = new LinkedList<>();
    for (final Set<RuntimeEdge> edgeSet : stageBoundaryRuntimeEdges.values()) {
      edgeSet.forEach(runtimeEdge -> {
        final RuntimeVertex endpointVertex;
        if (isIncomingEdges) {
          endpointVertex = runtimeEdge.getSrcRuntimeVertex();
        } else {
          endpointVertex = runtimeEdge.getDstRuntimeVertex();
        }
        stageBoundaryEdgeInfos.add(new StageBoundaryEdgeInfo(runtimeEdge.getId(), runtimeEdge.getEdgeAttributes(),
            endpointVertex.getId(), endpointVertex.getVertexAttributes()));
      });
    }
    return stageBoundaryEdgeInfos;
  }
}
