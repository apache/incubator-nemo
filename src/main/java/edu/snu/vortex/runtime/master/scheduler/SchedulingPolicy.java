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

import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.master.ExecutorRepresenter;

import java.util.Optional;

/**
 * Defines the policy by which {@link Scheduler} assigns task groups to executors.
 */
public interface SchedulingPolicy {

  /**
   * Returns this scheduling policy's timeout before an executor assignment.
   * @return the timeout in milliseconds.
   */
  long getScheduleTimeout();

  /**
   * Attempts to schedule the given taskGroup to an executor according to this policy.
   * If there is no executor available for the taskGroup, it waits for an executor to be assigned before it times out.
   * (Depending on the executor's resource type)
   *
   * @param taskGroup to schedule
   * @return executorId on which the taskGroup is scheduled if successful, an empty Optional otherwise.
   */
  Optional<String> attemptSchedule(final TaskGroup taskGroup);

  /**
   * Adds the executor to the pool of available executors.
   * Unlocks this policy to schedule a next taskGroup if locked.
   * (Depending on the executor's resource type)
   *
   * @param executor that has been added.
   */
  void onExecutorAdded(final ExecutorRepresenter executor);

  /**
   * Deletes the executor from the pool of available executors.
   * Locks this policy from scheduling if there is no more executor currently available for the next taskGroup.
   * (Depending on the executor's resource type)
   *
   * @param executor that has been deleted.
   */
  void onExecutorDeleted(final ExecutorRepresenter executor);

  /**
   * Marks the executor scheduled for the taskGroup.
   * Locks this policy from scheduling if there is no more executor currently available for the next taskGroup.
   * (Depending on the executor's resource type)
   *
   * @param executor assigned for the taskGroup.
   * @param taskGroup scheduled to the executor.
   */
  void onTaskGroupScheduled(final ExecutorRepresenter executor, final TaskGroup taskGroup);

  /**
   * Tentative.
   * @param executor .
   * @param taskGroup .
   */
  void onTaskGroupLaunched(final ExecutorRepresenter executor, final TaskGroup taskGroup);

  /**
   * Marks the taskGroup's completion in the executor.
   * Unlocks this policy to schedule a next taskGroup if locked.
   * (Depending on the executor's resource type)
   *
   * @param executor where the taskGroup's execution has completed.
   * @param taskGroup whose execution has completed.
   */
  void onTaskGroupExecutionComplete(final ExecutorRepresenter executor, final TaskGroup taskGroup);
}
