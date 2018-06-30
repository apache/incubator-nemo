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
package edu.snu.nemo.runtime.master.scheduler;

import edu.snu.nemo.runtime.common.plan.Stage;
import edu.snu.nemo.runtime.common.state.StageState;
import edu.snu.nemo.runtime.common.state.TaskState;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;

import java.util.Optional;

/**
 * Utility class for runtime unit tests.
 */
final class SchedulerTestUtil {
  /**
   * Complete the stage by completing all of its Tasks.
   * @param jobStateManager for the submitted job.
   * @param scheduler for the submitted job.
   * @param executorRegistry provides executor representers
   * @param stage for which the states should be marked as complete.
   */
  static void completeStage(final JobStateManager jobStateManager,
                            final Scheduler scheduler,
                            final ExecutorRegistry executorRegistry,
                            final Stage stage,
                            final int attemptIdx) {
    // Loop until the stage completes.
    while (true) {
      final StageState.State stageState = jobStateManager.getStageState(stage.getId());
      if (StageState.State.COMPLETE == stageState) {
        // Stage has completed, so we break out of the loop.
        break;
      } else if (StageState.State.INCOMPLETE == stageState) {
        stage.getTaskIds().forEach(taskId -> {
          final TaskState.State taskState = jobStateManager.getTaskState(taskId);
          if (TaskState.State.EXECUTING == taskState) {
            sendTaskStateEventToScheduler(scheduler, executorRegistry, taskId,
                TaskState.State.COMPLETE, attemptIdx, null);
          } else if (TaskState.State.READY == taskState || TaskState.State.COMPLETE == taskState) {
            // Skip READY (try in the next loop and see if it becomes EXECUTING) and COMPLETE.
          } else {
            throw new IllegalStateException(taskState.toString());
          }
        });
      } else {
        throw new IllegalStateException(stageState.toString());
      }
    }
  }

  /**
   * Sends task state change event to scheduler.
   * This replaces executor's task completion messages for testing purposes.
   * @param scheduler for the submitted job.
   * @param executorRegistry provides executor representers
   * @param taskId for the task to change the state.
   * @param newState for the task.
   * @param cause in the case of a recoverable failure.
   */
  static void sendTaskStateEventToScheduler(final Scheduler scheduler,
                                            final ExecutorRegistry executorRegistry,
                                            final String taskId,
                                            final TaskState.State newState,
                                            final int attemptIdx,
                                            final TaskState.RecoverableTaskFailureCause cause) {
    final ExecutorRepresenter scheduledExecutor;
    while (true) {
      final Optional<ExecutorRepresenter> optional = executorRegistry.findExecutorForTask(taskId);
      if (optional.isPresent()) {
        scheduledExecutor = optional.get();
        break;
      }
    }
    scheduler.onTaskStateReportFromExecutor(scheduledExecutor.getExecutorId(), taskId, attemptIdx,
        newState, null, cause);
  }

  static void sendTaskStateEventToScheduler(final Scheduler scheduler,
                                            final ExecutorRegistry executorRegistry,
                                            final String taskId,
                                            final TaskState.State newState,
                                            final int attemptIdx) {
    sendTaskStateEventToScheduler(scheduler, executorRegistry, taskId, newState, attemptIdx, null);
  }
}
