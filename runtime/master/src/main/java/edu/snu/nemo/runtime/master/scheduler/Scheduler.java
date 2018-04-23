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

import edu.snu.nemo.common.Pair;
import edu.snu.nemo.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.nemo.runtime.common.state.TaskGroupState;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.DefaultImplementation;

import javax.annotation.Nullable;

/**
 * Only two threads call scheduling code: RuntimeMaster thread (RMT), and SchedulerThread(ST).
 * RMT and ST meet only at two points: SchedulingPolicy, and PendingTaskGroupCollection,
 * which are synchronized(ThreadSafe).
 * Other scheduler-related classes that are accessed by only one of the two threads are not synchronized(NotThreadSafe).
 *
 * Receives jobs to execute and schedules
 * {@link edu.snu.nemo.runtime.common.plan.physical.ScheduledTaskGroup} to executors.
 */
@DriverSide
@DefaultImplementation(BatchSingleJobScheduler.class)
public interface Scheduler {

  /**
   * Schedules the given job.
   * @param physicalPlan of the job being submitted.
   * @param jobStateManager to manage the states of the submitted job.
   */
  void scheduleJob(PhysicalPlan physicalPlan,
                   JobStateManager jobStateManager);

  /**
   * Receives and updates the scheduler with a new physical plan for a job.
   * @param jobId the ID of the job to change the physical plan.
   * @param newPhysicalPlan new physical plan for the job.
   * @param taskInfo pair containing the information of the executor id and task group id to mark as complete after the
   *                 update.
   */
  void updateJob(String jobId, PhysicalPlan newPhysicalPlan, Pair<String, String> taskInfo);

  /**
   * Called when an executor is added to Runtime, so that the extra resource can be used to execute the job.
   * @param executorRepresenter a representation of the added executor.
   */
  void onExecutorAdded(ExecutorRepresenter executorRepresenter);

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
   * @param taskPutOnHold the ID of task that are put on hold. It is null otherwise.
   * @param failureCause for which the TaskGroup failed in the case of a recoverable failure.
   */
  void onTaskGroupStateChanged(String executorId,
                               String taskGroupId,
                               TaskGroupState.State newState,
                               int attemptIdx,
                               @Nullable String taskPutOnHold,
                               TaskGroupState.RecoverableFailureCause failureCause);

  /**
   * To be called when a job should be terminated.
   * Any clean up code should be implemented in this method.
   */
  void terminate();
}
