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
package edu.snu.nemo.tests.runtime;

import edu.snu.nemo.runtime.common.plan.physical.*;
import edu.snu.nemo.runtime.common.state.StageState;
import edu.snu.nemo.runtime.common.state.TaskGroupState;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.master.scheduler.ExecutorRegistry;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import edu.snu.nemo.runtime.master.scheduler.PendingTaskGroupQueue;
import edu.snu.nemo.runtime.master.scheduler.Scheduler;
import edu.snu.nemo.runtime.master.scheduler.SchedulingPolicy;
import org.apache.beam.sdk.values.KV;

import java.util.*;
import java.util.stream.Collectors;
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
   * @param executorRegistry provides executor representers
   * @param physicalStage for which the states should be marked as complete.
   */
  public static void sendStageCompletionEventToScheduler(final JobStateManager jobStateManager,
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

  public static void mockSchedulerRunner(final PendingTaskGroupQueue pendingTaskGroupQueue,
                                         final SchedulingPolicy schedulingPolicy,
                                         final JobStateManager jobStateManager,
                                         final boolean isPartialSchedule) {
    while (!pendingTaskGroupQueue.isEmpty()) {
      final ScheduledTaskGroup taskGroupToSchedule = pendingTaskGroupQueue.dequeue().get();

      schedulingPolicy.scheduleTaskGroup(taskGroupToSchedule, jobStateManager);

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

  /**
   * Gets a list of integer pair elements in range.
   * @param start value of the range (inclusive).
   * @param end   value of the range (exclusive).
   * @return the list of elements.
   */
  public static List getRangedNumList(final int start,
                                               final int end) {
    final List numList = new ArrayList<>(end - start);
    IntStream.range(start, end).forEach(number -> numList.add(KV.of(number, number)));
    return numList;
  }

  /**
   * Flattens a nested list of elements.
   *
   * @param listOfList to flattens.
   * @return the flattened list of elements.
   */
  public static List flatten(final List<List> listOfList) {
    return listOfList.stream().flatMap(list -> ((List<Object>) list).stream()).collect(Collectors.toList());
  }
}
