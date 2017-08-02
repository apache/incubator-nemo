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
package edu.snu.vortex.runtime;

import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.runtime.common.state.PartitionState;
import edu.snu.vortex.runtime.common.state.StageState;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.runtime.master.JobStateManager;
import edu.snu.vortex.runtime.master.PartitionManagerMaster;
import edu.snu.vortex.runtime.master.resource.ContainerManager;
import edu.snu.vortex.runtime.master.resource.ExecutorRepresenter;
import edu.snu.vortex.runtime.master.scheduler.*;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

/**
 * Utility class for runtime unit tests.
 */
public final class RuntimeTestUtil {

  /**
   * Sends a stage's completion event to scheduler, with all its task groups marked as complete as well.
   * This replaces executor's task group completion messages for testing purposes.
   * @param jobStateManager for the submitted job.
   * @param scheduler for the submitted job.
   * @param containerManager used for testing purposes.
   * @param physicalStage for which the states should be marked as complete.
   */
  public static void sendStageCompletionEventToScheduler(final JobStateManager jobStateManager,
                                                         final Scheduler scheduler,
                                                         final ContainerManager containerManager,
                                                         final PhysicalStage physicalStage,
                                                         final int attemptIdx) {
    while (jobStateManager.getStageState(physicalStage.getId()).getStateMachine().getCurrentState()
        == StageState.State.EXECUTING) {
      physicalStage.getTaskGroupList().forEach(taskGroup -> {
        if (jobStateManager.getTaskGroupState(taskGroup.getTaskGroupId()).getStateMachine().getCurrentState()
            == TaskGroupState.State.EXECUTING) {
          sendTaskGroupStateEventToScheduler(scheduler, containerManager, taskGroup.getTaskGroupId(),
              TaskGroupState.State.COMPLETE, attemptIdx, null);
        }
      });
    }
  }

  /**
   * Sends task group state change event to scheduler.
   * This replaces executor's task group completion messages for testing purposes.
   * @param scheduler for the submitted job.
   * @param containerManager used for testing purposes.
   * @param taskGroupId for the task group to change the state.
   * @param newState for the task group.
   * @param cause in the case of a recoverable failure.
   */
  public static void sendTaskGroupStateEventToScheduler(final Scheduler scheduler,
                                                        final ContainerManager containerManager,
                                                        final String taskGroupId,
                                                        final TaskGroupState.State newState,
                                                        final int attemptIdx,
                                                        final TaskGroupState.RecoverableFailureCause cause) {
    ExecutorRepresenter scheduledExecutor;
    do {
      scheduledExecutor = findExecutorForTaskGroup(containerManager, taskGroupId);
    } while (scheduledExecutor == null);

    scheduler.onTaskGroupStateChanged(scheduledExecutor.getExecutorId(), taskGroupId,
        newState, attemptIdx, Collections.emptyList(), cause);
  }

  /**
   * Sends partition state changes of a stage to PartitionManager.
   * This replaces executor's partition state messages for testing purposes.
   * @param partitionManagerMaster used for testing purposes.
   * @param containerManager used for testing purposes.
   * @param stageOutgoingEdges to infer partition IDs.
   * @param physicalStage to change the state.
   * @param newState for the task group.
   */
  public static void sendPartitionStateEventForAStage(final PartitionManagerMaster partitionManagerMaster,
                                                      final ContainerManager containerManager,
                                                      final List<PhysicalStageEdge> stageOutgoingEdges,
                                                      final PhysicalStage physicalStage,
                                                      final PartitionState.State newState) {
    final List<TaskGroup> taskGroupsForStage = physicalStage.getTaskGroupList();

    // Initialize states for blocks of inter-stage edges
    stageOutgoingEdges.forEach(physicalStageEdge -> {
      final Attribute commPattern =
          physicalStageEdge.getAttributes().get(Attribute.Key.CommunicationPattern);
      final int srcParallelism = taskGroupsForStage.size();
      IntStream.range(0, srcParallelism).forEach(srcTaskIdx -> {
        if (commPattern == Attribute.ScatterGather) {
          final int dstParallelism =
              physicalStageEdge.getDstVertex().getAttributes().get(Attribute.IntegerKey.Parallelism);
          IntStream.range(0, dstParallelism).forEach(dstTaskIdx -> {
            final String partitionId =
                RuntimeIdGenerator.generatePartitionId(physicalStageEdge.getId(), srcTaskIdx, dstTaskIdx);
            sendPartitionStateEventToPartitionManager(partitionManagerMaster, containerManager, partitionId, newState);
          });
        } else {
          final String partitionId =
              RuntimeIdGenerator.generatePartitionId(physicalStageEdge.getId(), srcTaskIdx);
          sendPartitionStateEventToPartitionManager(partitionManagerMaster, containerManager, partitionId, newState);
        }
      });
    });

    // Initialize states for blocks of stage internal edges
    taskGroupsForStage.forEach(taskGroup -> {
      final DAG<Task, RuntimeEdge<Task>> taskGroupInternalDag = taskGroup.getTaskDAG();
      taskGroupInternalDag.getVertices().forEach(task -> {
        final List<RuntimeEdge<Task>> internalOutgoingEdges = taskGroupInternalDag.getOutgoingEdgesOf(task);
        internalOutgoingEdges.forEach(taskRuntimeEdge -> {
          final String partitionId =
              RuntimeIdGenerator.generatePartitionId(taskRuntimeEdge.getId(), taskGroup.getTaskGroupIdx());
          sendPartitionStateEventToPartitionManager(partitionManagerMaster, containerManager, partitionId, newState);
        });
      });
    });
  }

  /**
   * Sends partition state change event to PartitionManager.
   * This replaces executor's partition state messages for testing purposes.
   * @param partitionManagerMaster used for testing purposes.
   * @param containerManager used for testing purposes.
   * @param partitionId for the partition to change the state.
   * @param newState for the task group.
   */
  public static void sendPartitionStateEventToPartitionManager(final PartitionManagerMaster partitionManagerMaster,
                                                               final ContainerManager containerManager,
                                                               final String partitionId,
                                                               final PartitionState.State newState) {
    final Optional<String> parentTaskGroupId = partitionManagerMaster.getProducerTaskGroupId(partitionId);
    if (parentTaskGroupId.isPresent()) {
      final ExecutorRepresenter scheduledExecutor = findExecutorForTaskGroup(containerManager, parentTaskGroupId.get());


      if (scheduledExecutor != null) {
        partitionManagerMaster.onPartitionStateChanged(partitionId, newState, scheduledExecutor.getExecutorId());
      }
    }
  }

  /**
   * Retrieves the executor to which the given task group was scheduled.
   * @param taskGroupId of the task group to search.
   * @param containerManager used for testing purposes.
   * @return the {@link ExecutorRepresenter} of the executor the task group was scheduled to.
   */
  private static ExecutorRepresenter findExecutorForTaskGroup(final ContainerManager containerManager,
                                                              final String taskGroupId) {
    for (final ExecutorRepresenter executor : containerManager.getExecutorRepresenterMap().values()) {
      if (executor.getRunningTaskGroups().contains(taskGroupId)
          || executor.getCompleteTaskGroups().contains(taskGroupId)) {
        return executor;
      }
    }
    return null;
  }
}
