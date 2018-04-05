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

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.nemo.runtime.common.plan.physical.PhysicalStage;
import edu.snu.nemo.runtime.common.plan.physical.PhysicalStageEdge;
import edu.snu.nemo.runtime.common.plan.physical.ScheduledTaskGroup;
import net.jcip.annotations.ThreadSafe;
import org.apache.reef.annotations.audience.DriverSide;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;

/**
 * {@link PendingTaskGroupCollection} implementation.
 * This class provides two-level scheduling by keeping track of schedulable stages and stage-TaskGroup membership.
 * {@link #peekSchedulableTaskGroups()} returns collection of TaskGroups which belong to one of the schedulable stages.
 */
@ThreadSafe
@DriverSide
public final class SingleJobTaskGroupCollection implements PendingTaskGroupCollection {
  private PhysicalPlan physicalPlan;

  /**
   * Pending TaskGroups awaiting to be scheduled for each stage.
   */
  private final ConcurrentMap<String, Map<String, ScheduledTaskGroup>> stageIdToPendingTaskGroups;

  /**
   * Stages with TaskGroups that have not yet been scheduled.
   */
  private final BlockingDeque<String> schedulableStages;

  @Inject
  public SingleJobTaskGroupCollection() {
    stageIdToPendingTaskGroups = new ConcurrentHashMap<>();
    schedulableStages = new LinkedBlockingDeque<>();
  }

  @Override
  public synchronized void add(final ScheduledTaskGroup scheduledTaskGroup) {
    final String stageId = RuntimeIdGenerator.getStageIdFromTaskGroupId(scheduledTaskGroup.getTaskGroupId());

    stageIdToPendingTaskGroups.compute(stageId, (s, taskGroupIdToTaskGroup) -> {
      if (taskGroupIdToTaskGroup == null) {
        final Map<String, ScheduledTaskGroup> taskGroupIdToTaskGroupMap = new HashMap<>();
        taskGroupIdToTaskGroupMap.put(scheduledTaskGroup.getTaskGroupId(), scheduledTaskGroup);
        updateSchedulableStages(stageId, scheduledTaskGroup.getContainerType());
        return taskGroupIdToTaskGroupMap;
      } else {
        taskGroupIdToTaskGroup.put(scheduledTaskGroup.getTaskGroupId(), scheduledTaskGroup);
        return taskGroupIdToTaskGroup;
      }
    });
  }

  /**
   * Removes the specified TaskGroup to be scheduled.
   * The specified TaskGroup should belong to the collection from {@link #peekSchedulableTaskGroups()}.
   * @param taskGroupId id of the TaskGroup
   * @return the specified TaskGroup
   * @throws NoSuchElementException if the specified TaskGroup is not in the queue,
   *                                or removing this TaskGroup breaks scheduling order
   *                                (i.e. does not belong to the collection from {@link #peekSchedulableTaskGroups()}.
   */
  @Override
  public synchronized ScheduledTaskGroup remove(final String taskGroupId) throws NoSuchElementException {
    final String stageId = schedulableStages.peekFirst();
    if (stageId == null) {
      throw new NoSuchElementException("No schedulable stage in TaskGroup queue");
    }

    final Map<String, ScheduledTaskGroup> pendingTaskGroupsForStage = stageIdToPendingTaskGroups.get(stageId);

    if (pendingTaskGroupsForStage == null) {
      throw new RuntimeException(String.format("Stage %s not found in TaskGroup queue", stageId));
    }
    final ScheduledTaskGroup taskGroupToSchedule = pendingTaskGroupsForStage.remove(taskGroupId);
    if (taskGroupToSchedule == null) {
      throw new NoSuchElementException(String.format("TaskGroup %s not found in TaskGroup queue", taskGroupId));
    }
    if (pendingTaskGroupsForStage.isEmpty()) {
      if (!schedulableStages.pollFirst().equals(stageId)) {
        throw new RuntimeException(String.format("Expected stage %s to be polled", stageId));
      }
      stageIdToPendingTaskGroups.remove(stageId);
      stageIdToPendingTaskGroups.forEach((scheduledStageId, taskGroups) ->
          updateSchedulableStages(scheduledStageId, taskGroups.values().iterator().next().getContainerType()));
    }

    return taskGroupToSchedule;
  }

  /**
   * Peeks TaskGroups that can be scheduled.
   * @return TaskGroups to be scheduled, or {@link Optional#empty()} if the queue is empty
   * @return collection of TaskGroups which belong to one of the schedulable stages
   *         or {@link Optional#empty} if the queue is empty
   */
  @Override
  public synchronized Optional<Collection<ScheduledTaskGroup>> peekSchedulableTaskGroups() {
    final String stageId = schedulableStages.peekFirst();
    if (stageId == null) {
      return Optional.empty();
    }

    final Map<String, ScheduledTaskGroup> pendingTaskGroupsForStage = stageIdToPendingTaskGroups.get(stageId);
    if (pendingTaskGroupsForStage == null) {
      throw new RuntimeException(String.format("Stage %s not found in stageIdToPendingTaskGroups map", stageId));
    }
    return Optional.of(new ArrayList<>(pendingTaskGroupsForStage.values()));
  }

  /**
   * Removes a stage and its descendant stages from this PQ.
   * @param stageId for the stage to begin the removal recursively.
   */
  @Override
  public synchronized void removeTaskGroupsAndDescendants(final String stageId) {
    removeStageAndChildren(stageId);
  }

  /**
   * Recursively removes a stage and its children stages from this PQ.
   * @param stageId for the stage to begin the removal recursively.
   */
  private synchronized void removeStageAndChildren(final String stageId) {
    if (schedulableStages.remove(stageId)) {
      stageIdToPendingTaskGroups.remove(stageId);
    }

    physicalPlan.getStageDAG().getChildren(stageId).forEach(
        physicalStage -> removeStageAndChildren(physicalStage.getId()));
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
    final DAG<PhysicalStage, PhysicalStageEdge> jobDAG = physicalPlan.getStageDAG();

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
    final DAG<PhysicalStage, PhysicalStageEdge> jobDAG = physicalPlan.getStageDAG();
    for (final PhysicalStage descendantStage : jobDAG.getDescendants(candidateStageId)) {
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
    stageIdToPendingTaskGroups.clear();
  }
}
