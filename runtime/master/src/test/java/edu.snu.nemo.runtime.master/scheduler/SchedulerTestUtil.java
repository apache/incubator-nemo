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
package edu.snu.nemo.runtime.master.scheduler;

import edu.snu.nemo.runtime.common.plan.physical.PhysicalStage;
import edu.snu.nemo.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.nemo.runtime.common.state.StageState;
import edu.snu.nemo.runtime.common.state.TaskGroupState;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class for runtime unit tests.
 */
public final class SchedulerTestUtil {
  /**
   * Complete the stage by completing all of its TaskGroups.
   * @param jobStateManager for the submitted job.
   * @param scheduler for the submitted job.
   * @param executorRegistry provides executor representers
   * @param physicalStage for which the states should be marked as complete.
   */
  public static void completeStage(final JobStateManager jobStateManager,
                                   final Scheduler scheduler,
                                   final ExecutorRegistry executorRegistry,
                                   final PhysicalStage physicalStage,
                                   final int attemptIdx) {
    // Loop until the stage completes.
    while (true) {
      final Enum stageState = jobStateManager.getStageState(physicalStage.getId()).getStateMachine().getCurrentState();
      if (StageState.State.COMPLETE == stageState) {
        // Stage has completed, so we break out of the loop.
        break;
      } else if (StageState.State.EXECUTING == stageState) {
        physicalStage.getTaskGroupIds().forEach(taskGroupId -> {
          final Enum tgState = jobStateManager.getTaskGroupState(taskGroupId).getStateMachine().getCurrentState();
          if (TaskGroupState.State.EXECUTING == tgState) {
            sendTaskGroupStateEventToScheduler(scheduler, executorRegistry, taskGroupId,
                TaskGroupState.State.COMPLETE, attemptIdx, null);
          } else if (TaskGroupState.State.READY == tgState || TaskGroupState.State.COMPLETE == tgState) {
            // Skip READY (try in the next loop and see if it becomes EXECUTING) and COMPLETE.
          } else {
            throw new IllegalStateException(tgState.toString());
          }
        });
      } else if (StageState.State.READY == stageState) {
        // Skip and retry in the next loop.
      } else {
        throw new IllegalStateException(stageState.toString());
      }
    }
  }

  /**
   * Sends task group state change event to scheduler.
   * This replaces executor's task group completion messages for testing purposes.
   * @param scheduler for the submitted job.
   * @param executorRegistry provides executor representers
   * @param taskGroupId for the task group to change the state.
   * @param newState for the task group.
   * @param cause in the case of a recoverable failure.
   */
  public static void sendTaskGroupStateEventToScheduler(final Scheduler scheduler,
                                                        final ExecutorRegistry executorRegistry,
                                                        final String taskGroupId,
                                                        final TaskGroupState.State newState,
                                                        final int attemptIdx,
                                                        final TaskGroupState.RecoverableFailureCause cause) {
    ExecutorRepresenter scheduledExecutor;
    do {
      scheduledExecutor = findExecutorForTaskGroup(executorRegistry, taskGroupId);
    } while (scheduledExecutor == null);

    scheduler.onTaskGroupStateChanged(scheduledExecutor.getExecutorId(), taskGroupId,
        newState, attemptIdx, null, cause);
  }

  public static void sendTaskGroupStateEventToScheduler(final Scheduler scheduler,
                                                        final ExecutorRegistry executorRegistry,
                                                        final String taskGroupId,
                                                        final TaskGroupState.State newState,
                                                        final int attemptIdx) {
    sendTaskGroupStateEventToScheduler(scheduler, executorRegistry, taskGroupId, newState, attemptIdx, null);
  }

  public static void mockSchedulerRunner(final PendingTaskGroupCollection pendingTaskGroupCollection,
                                         final SchedulingPolicy schedulingPolicy,
                                         final JobStateManager jobStateManager,
                                         final ExecutorRegistry executorRegistry,
                                         final boolean isPartialSchedule) {
    while (!pendingTaskGroupCollection.isEmpty()) {
      final ScheduledTaskGroup taskGroupToSchedule = pendingTaskGroupCollection.remove(
          pendingTaskGroupCollection.peekSchedulableTaskGroups().get().iterator().next().getTaskGroupId());

      final Set<ExecutorRepresenter> runningExecutorRepresenter =
          executorRegistry.getRunningExecutorIds().stream()
              .map(executorId -> executorRegistry.getExecutorRepresenter(executorId))
              .collect(Collectors.toSet());
      final Set<ExecutorRepresenter> candidateExecutors =
          schedulingPolicy.filterExecutorRepresenters(runningExecutorRepresenter, taskGroupToSchedule);
      if (candidateExecutors.size() > 0) {
        jobStateManager.onTaskGroupStateChanged(taskGroupToSchedule.getTaskGroupId(),
            TaskGroupState.State.EXECUTING);
        final ExecutorRepresenter executor = candidateExecutors.stream().findFirst().get();
        executor.onTaskGroupScheduled(taskGroupToSchedule);
      }

      // Schedule only the first task group.
      if (isPartialSchedule) {
        break;
      }
    }
  }

  /**
   * Retrieves the executor to which the given task group was scheduled.
   * @param taskGroupId of the task group to search.
   * @param executorRegistry provides executor representers
   * @return the {@link ExecutorRepresenter} of the executor the task group was scheduled to.
   */
  private static ExecutorRepresenter findExecutorForTaskGroup(final ExecutorRegistry executorRegistry,
                                                              final String taskGroupId) {
    for (final String executorId : executorRegistry.getRunningExecutorIds()) {
      final ExecutorRepresenter executor = executorRegistry.getRunningExecutorRepresenter(executorId);
      if (executor.getRunningTaskGroups().contains(taskGroupId)
          || executor.getCompleteTaskGroups().contains(taskGroupId)) {
        return executor;
      }
    }
    return null;
  }
}
