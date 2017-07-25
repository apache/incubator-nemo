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
package edu.snu.vortex.runtime.master.scheduler;

import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.runtime.master.JobStateManager;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;

/**
 * Receives a job to execute and schedules {@link edu.snu.vortex.runtime.common.plan.physical.TaskGroup} to executors.
 */
@DefaultImplementation(BatchScheduler.class)
public interface Scheduler {

  /**
   * Schedules the given job.
   * @param physicalPlan of the job being submitted.
   * @param maxScheduleAttempt the max. number of times a stage can be attempted for execution.
   * @return the {@link JobStateManager} for the submitted job to keep track of the execution states.
   */
  JobStateManager scheduleJob(final PhysicalPlan physicalPlan,
                              final int maxScheduleAttempt);

  /**
   * Called when an executor is added to Runtime, so that the extra resource can be used to execute the job.
   * @param executorId of the executor that has been added.
   */
  void onExecutorAdded(String executorId);

  /**
   * Called when an executor is removed from Runtime, so that faults related to the removal can be handled.
   * @param executorId of the executor that has been removed.
   */
  void onExecutorRemoved(String executorId);

  /**
   * Called when a TaskGroup's execution state changes.
   * @param executorId of the executor in which the TaskGroup is executing.
   * @param taskGroupId of the TaskGroup whose state must be updated.
   * @param newState for the TaskGroup.
   * @param attemptIdx the number of times this TaskGroup has executed.
   *************** the below parameters are only valid for failures *****************
   * @param failedTaskIds the IDs of the failed Tasks of the TaskGroup upon failure.
   * @param failureCause for which the TaskGroup failed in the case of a recoverable failure.
   */
  void onTaskGroupStateChanged(final String executorId,
                               final String taskGroupId,
                               final TaskGroupState.State newState,
                               final int attemptIdx,
                               final List<String> failedTaskIds,
                               final TaskGroupState.RecoverableFailureCause failureCause);

  /**
   * To be called when a job should be terminated.
   * Any clean up code should be implemented in this method.
   */
  void terminate();
}
