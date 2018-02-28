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

import edu.snu.nemo.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.nemo.runtime.common.plan.physical.ScheduledTaskGroup;

import net.jcip.annotations.ThreadSafe;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Optional;

/**
 * Keep tracks of all pending task groups.
 * {@link Scheduler} enqueues the TaskGroups to schedule to this queue.
 * {@link SchedulerRunner} refers to this queue when scheduling TaskGroups.
 */
@ThreadSafe
@DriverSide
@DefaultImplementation(SingleJobTaskGroupQueue.class)
public interface PendingTaskGroupQueue {

  /**
   * Enqueues a TaskGroup to this PQ.
   * @param scheduledTaskGroup to enqueue.
   */
  void enqueue(final ScheduledTaskGroup scheduledTaskGroup);

  /**
   * Dequeues the next TaskGroup to be scheduled.
   * @return an optional of the the next TaskGroup to be scheduled,
   * an empty optional if no such TaskGroup exists.
   */
  Optional<ScheduledTaskGroup> dequeue();

  /**
   * Registers a job to this queue in case the queue needs to understand the topology of the job DAG.
   * @param physicalPlanForJob the job to schedule.
   */
  void onJobScheduled(final PhysicalPlan physicalPlanForJob);

  /**
   * Removes a stage and its descendant stages from this queue.
   * This is to be used for fault tolerance purposes,
   * say when a stage fails and all affected TaskGroups must be removed.
   * @param stageIdOfTaskGroups for the stage to begin the removal recursively.
   */
  void removeTaskGroupsAndDescendants(final String stageIdOfTaskGroups);

  /**
   * Checks whether there are schedulable TaskGroups in the queue or not.
   * @return true if there are schedulable TaskGroups in the queue, false otherwise.
   */
  boolean isEmpty();

  /**
   * Closes and cleans up this queue.
   */
  void close();
}
