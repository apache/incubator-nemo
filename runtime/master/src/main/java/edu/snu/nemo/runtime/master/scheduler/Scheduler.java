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

import edu.snu.nemo.runtime.common.plan.PhysicalPlan;
import edu.snu.nemo.runtime.common.state.TaskState;
import edu.snu.nemo.runtime.master.PlanStateManager;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.DefaultImplementation;

import javax.annotation.Nullable;

/**
 * Only two threads call scheduling code: RuntimeMaster thread (RMT), and SchedulerThread(ST).
 * RMT and ST meet only at two points: {@link ExecutorRegistry}, and {@link PendingTaskCollectionPointer},
 * which are synchronized(ThreadSafe).
 * Other scheduler-related classes that are accessed by only one of the two threads are not synchronized(NotThreadSafe).
 */
@DriverSide
@DefaultImplementation(BatchScheduler.class)
public interface Scheduler {

  /**
   * Schedules the given plan.
   * @param physicalPlan of the job being submitted.
   * @param planStateManager to manage the states of the submitted plan.
   */
  void schedulePlan(PhysicalPlan physicalPlan,
                    PlanStateManager planStateManager);

  /**
   * Receives and updates the scheduler with a new physical plan for a job.
   * @param planId the ID of the physical plan to change.
   * @param newPhysicalPlan new physical plan for the job.
   */
  void updatePlan(String planId, PhysicalPlan newPhysicalPlan);

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
   * Called when a Task's execution state changes.
   * @param executorId of the executor in which the Task is executing.
   * @param taskId of the Task whose state must be updated.
   * @param newState for the Task.
   * @param attemptIdx the number of times this Task has executed.
   *************** the below parameters are only valid for failures *****************
   * @param taskPutOnHold the ID of task that are put on hold. It is null otherwise.
   * @param failureCause for which the Task failed in the case of a recoverable failure.
   */
  void onTaskStateReportFromExecutor(String executorId,
                                     String taskId,
                                     int attemptIdx,
                                     TaskState.State newState,
                                     @Nullable String taskPutOnHold,
                                     TaskState.RecoverableTaskFailureCause failureCause);

  /**
   * To be called when a job should be terminated.
   * Any clean up code should be implemented in this method.
   */
  void terminate();
}
