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
import edu.snu.nemo.runtime.common.plan.Task;

import net.jcip.annotations.ThreadSafe;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * Keep tracks of all pending tasks.
 * {@link Scheduler} enqueues the Tasks to schedule to this queue.
 * {@link SchedulerRunner} refers to this queue when scheduling Tasks.
 */
@ThreadSafe
@DriverSide
@DefaultImplementation(SingleJobTaskCollection.class)
public interface PendingTaskCollection {

  /**
   * Adds a Task to this collection.
   * @param task to add.
   */
  void add(final Task task);

  /**
   * Removes the specified Task to be scheduled.
   * @param taskId id of the Task
   * @return the specified Task
   * @throws NoSuchElementException if the specified Task is not in the queue,
   *                                or removing this Task breaks scheduling order
   */
  Task remove(final String taskId) throws NoSuchElementException;

  /**
   * Peeks stage that can be scheduled according to job dependency priority.
   * Changes to the queue must not reflected to the returned collection to avoid concurrent modification.
   * @return stage that can be scheduled, or {@link Optional#empty()} if the queue is empty
   */
  Optional<Collection<Task>> peekSchedulableStage();

  /**
   * Registers a job to this queue in case the queue needs to understand the topology of the job DAG.
   * @param physicalPlanForJob the job to schedule.
   */
  void onJobScheduled(final PhysicalPlan physicalPlanForJob);

  /**
   * Removes a stage and its descendant stages from this queue.
   * This is to be used for fault tolerance purposes,
   * say when a stage fails and all affected Tasks must be removed.
   * @param stageIdOfTasks for the stage to begin the removal recursively.
   */
  void removeTasksAndDescendants(final String stageIdOfTasks);

  /**
   * Checks whether there are schedulable Tasks in the queue or not.
   * @return true if there are schedulable Tasks in the queue, false otherwise.
   */
  boolean isEmpty();

  /**
   * Closes and cleans up this queue.
   */
  void close();
}
