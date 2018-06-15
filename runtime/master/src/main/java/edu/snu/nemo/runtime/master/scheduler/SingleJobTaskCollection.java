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

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.PhysicalPlan;
import edu.snu.nemo.runtime.common.plan.Stage;
import edu.snu.nemo.runtime.common.plan.StageEdge;
import edu.snu.nemo.runtime.common.plan.Task;
import net.jcip.annotations.ThreadSafe;
import org.apache.reef.annotations.audience.DriverSide;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;

/**
 * {@link PendingTaskCollection} implementation.
 * This class provides two-level scheduling by keeping track of schedulable stages and stage-Task membership.
 * {@link #peekSchedulableStage()} returns collection of Tasks which belong to one of the schedulable stages.
 */
@ThreadSafe
@DriverSide
public final class SingleJobTaskCollection implements PendingTaskCollection {
  private PhysicalPlan physicalPlan;

  /**
   * Pending Tasks awaiting to be scheduled for each stage.
   */
  private final ConcurrentMap<String, Map<String, Task>> stageIdToPendingTasks;

  /**
   * Stages with Tasks that have not yet been scheduled.
   */
  private final BlockingDeque<String> schedulableStages;

  @Inject
  public SingleJobTaskCollection() {
    stageIdToPendingTasks = new ConcurrentHashMap<>();
    schedulableStages = new LinkedBlockingDeque<>();
  }

  @Override
  public synchronized void add(final Task task) {
    final String stageId = RuntimeIdGenerator.getStageIdFromTaskId(task.getTaskId());

    stageIdToPendingTasks.compute(stageId, (s, taskIdToTask) -> {
      if (taskIdToTask == null) {
        final Map<String, Task> taskIdToTaskMap = new HashMap<>();
        taskIdToTaskMap.put(task.getTaskId(), task);
        updateSchedulableStages(stageId, task.getContainerType());
        return taskIdToTaskMap;
      } else {
        taskIdToTask.put(task.getTaskId(), task);
        return taskIdToTask;
      }
    });
  }

  /**
   * Removes the specified Task to be scheduled.
   * The specified Task should belong to the collection from {@link #peekSchedulableStage()}.
   * @param taskId id of the Task
   * @return the specified Task
   * @throws NoSuchElementException if the specified Task is not in the queue,
   *                                or removing this Task breaks scheduling order
   *                                (i.e. does not belong to the collection from {@link #peekSchedulableStage()}.
   */
  @Override
  public synchronized Task remove(final String taskId) throws NoSuchElementException {
    final String stageId = schedulableStages.peekFirst();
    if (stageId == null) {
      throw new NoSuchElementException("No schedulable stage in Task queue");
    }

    final Map<String, Task> pendingTasksForStage = stageIdToPendingTasks.get(stageId);

    if (pendingTasksForStage == null) {
      throw new RuntimeException(String.format("Stage %s not found in Task queue", stageId));
    }
    final Task taskToSchedule = pendingTasksForStage.remove(taskId);
    if (taskToSchedule == null) {
      throw new NoSuchElementException(String.format("Task %s not found in Task queue", taskId));
    }
    if (pendingTasksForStage.isEmpty()) {
      if (!schedulableStages.pollFirst().equals(stageId)) {
        throw new RuntimeException(String.format("Expected stage %s to be polled", stageId));
      }
      stageIdToPendingTasks.remove(stageId);
      stageIdToPendingTasks.forEach((scheduledStageId, tasks) ->
          updateSchedulableStages(scheduledStageId, tasks.values().iterator().next().getContainerType()));
    }

    return taskToSchedule;
  }

  /**
   * Peeks Tasks that can be scheduled.
   * @return Tasks to be scheduled, or {@link Optional#empty()} if the queue is empty
   * @return collection of Tasks which belong to one of the schedulable stages
   *         or {@link Optional#empty} if the queue is empty
   */
  @Override
  public synchronized Optional<Collection<Task>> peekSchedulableStage() {
    final String stageId = schedulableStages.peekFirst();
    if (stageId == null) {
      return Optional.empty();
    }

    final Map<String, Task> pendingTasksForStage = stageIdToPendingTasks.get(stageId);
    if (pendingTasksForStage == null) {
      throw new RuntimeException(String.format("Stage %s not found in stageIdToPendingTasks map", stageId));
    }
    return Optional.of(new ArrayList<>(pendingTasksForStage.values()));
  }

  /**
   * Removes a stage and its descendant stages from this PQ.
   * @param stageId for the stage to begin the removal recursively.
   */
  @Override
  public synchronized void removeTasksAndDescendants(final String stageId) {
    removeStageAndChildren(stageId);
  }

  /**
   * Recursively removes a stage and its children stages from this PQ.
   * @param stageId for the stage to begin the removal recursively.
   */
  private synchronized void removeStageAndChildren(final String stageId) {
    if (schedulableStages.remove(stageId)) {
      stageIdToPendingTasks.remove(stageId);
    }

    physicalPlan.getStageDAG().getChildren(stageId).forEach(
        stage -> removeStageAndChildren(stage.getId()));
  }

  /**
   * Updates the two-level data structure by examining a new candidate stage.
   * If there are no stages with higher priority, the candidate can be made schedulable.
   *
   * NOTE: This method provides the "line up" between stages, by assigning priorities,
   * serving as the key to the "priority" implementation of this class.
   * @param candidateStageId for the stage that can potentially be scheduled.
   * @param candidateStageContainerType for the stage that can potentially be scheduled.
   */
  private synchronized void updateSchedulableStages(
      final String candidateStageId, final String candidateStageContainerType) {
    final DAG<Stage, StageEdge> jobDAG = physicalPlan.getStageDAG();

    if (isSchedulable(candidateStageId, candidateStageContainerType)) {
      // Check for ancestor stages that became schedulable due to candidateStage's absence from the queue.
      jobDAG.getAncestors(candidateStageId).forEach(ancestorStage -> {
        // Remove the ancestor stage if it is of the same container type.
        if (schedulableStages.contains(ancestorStage.getId())
            && candidateStageContainerType.equals(ancestorStage.getContainerType())) {
          if (!schedulableStages.remove(ancestorStage.getId())) {
            throw new RuntimeException(String.format("No such stage: %s", ancestorStage.getId()));
          }
        }
      });
      if (!schedulableStages.contains(candidateStageId)) {
        schedulableStages.addLast(candidateStageId);
      }
    }
  }

  /**
   * Determines whether the given candidate stage is schedulable immediately or not.
   * @param candidateStageId for the stage that can potentially be scheduled.
   * @param candidateStageContainerType for the stage that can potentially be scheduled.
   * @return true if schedulable, false otherwise.
   */
  private synchronized boolean isSchedulable(final String candidateStageId, final String candidateStageContainerType) {
    final DAG<Stage, StageEdge> jobDAG = physicalPlan.getStageDAG();
    for (final Stage descendantStage : jobDAG.getDescendants(candidateStageId)) {
      if (schedulableStages.contains(descendantStage.getId())) {
        if (candidateStageContainerType.equals(descendantStage.getContainerType())) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public synchronized void onJobScheduled(final PhysicalPlan physicalPlanForJob) {
    this.physicalPlan = physicalPlanForJob;
  }

  @Override
  public synchronized boolean isEmpty() {
    return schedulableStages.isEmpty();
  }

  @Override
  public synchronized void close() {
    schedulableStages.clear();
    stageIdToPendingTasks.clear();
  }
}
