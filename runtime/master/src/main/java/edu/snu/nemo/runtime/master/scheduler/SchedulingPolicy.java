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

import edu.snu.nemo.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.nemo.runtime.master.JobStateManager;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Set;

/**
 * Defines the policy by which {@link BatchSingleJobScheduler} assigns task groups to executors.
 */
@DriverSide
@DefaultImplementation(SourceLocationAwareSchedulingPolicy.class)
public interface SchedulingPolicy {

  /**
   * Returns this scheduling policy's timeout before an executor assignment.
   * @return the timeout in milliseconds.
   */
  long getScheduleTimeoutMs();

  /**
   * Attempts to schedule the given taskGroup to an executor according to this policy.
   * If there is no executor available for the taskGroup, it waits for an executor to be assigned before it times out.
   * (Depending on the executor's resource type)
   *
   * @param scheduledTaskGroup to schedule.
   * @param jobStateManager jobStateManager which the TaskGroup belongs to.
   * @return true if the task group is successfully scheduled, false otherwise.
   */
  boolean scheduleTaskGroup(final ScheduledTaskGroup scheduledTaskGroup, final JobStateManager jobStateManager);

  /**
   * Adds the executorId to the pool of available executors.
   * Unlocks this policy to schedule a next taskGroup if locked.
   * (Depending on the executor's resource type)
   *
   * @param executorId for the executor that has been added.
   */
  void onExecutorAdded(String executorId);

  /**
   * Deletes the executorId from the pool of available executors.
   * Locks this policy from scheduling if there is no more executor currently available for the next taskGroup.
   * (Depending on the executor's resource type)
   *
   * @param executorId for the executor that has been deleted.
   * @return the ids of the set of task groups that were running on the executor.
   */
  Set<String> onExecutorRemoved(String executorId);

  /**
   * Marks the taskGroup's completion in the executor.
   * Unlocks this policy to schedule a next taskGroup if locked.
   * (Depending on the executor's resource type)
   *
   * @param executorId of the executor where the taskGroup's execution has completed.
   * @param taskGroupId whose execution has completed.
   */
  void onTaskGroupExecutionComplete(String executorId, String taskGroupId);

  /**
   * Marks the taskGroup's failure in the executor.
   * Unlocks this policy to reschedule this taskGroup if locked.
   * (Depending on the executor's resource type)
   *
   * @param executorId of the executor where the taskGroup's execution has failed.
   * @param taskGroupId whose execution has completed.
   */
  void onTaskGroupExecutionFailed(String executorId, String taskGroupId);
}
