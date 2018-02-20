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
import edu.snu.nemo.common.exception.SchedulingException;
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
import java.util.function.BiFunction;

/**
 * Keep tracks of all pending task groups.
 * This class provides two-level queue scheduling by prioritizing TaskGroups of certain stages to be scheduled first.
 * Stages that are mutually independent alternate turns in scheduling each of their TaskGroups.
 * This PQ assumes that stages/task groups of higher priorities are never enqueued without first removing
 * those of lower priorities (which is how Scheduler behaves) for simplicity.
 */
@ThreadSafe
@DriverSide
public final class SingleJobTaskGroupQueue implements PendingTaskGroupQueue {
  private PhysicalPlan physicalPlan;

  /**
   * Pending TaskGroups awaiting to be scheduled for each stage.
   */
  private final ConcurrentMap<String, Deque<ScheduledTaskGroup>> stageIdToPendingTaskGroups;

  /**
   * Stages with TaskGroups that have not yet been scheduled.
   */
  private final BlockingDeque<String> schedulableStages;

  @Inject
  public SingleJobTaskGroupQueue() {
    stageIdToPendingTaskGroups = new ConcurrentHashMap<>();
    schedulableStages = new LinkedBlockingDeque<>();
  }

  @Override
  public void enqueue(final ScheduledTaskGroup scheduledTaskGroup) {
    final String stageId = RuntimeIdGenerator.getStageIdFromTaskGroupId(scheduledTaskGroup.getTaskGroupId());

    synchronized (stageIdToPendingTaskGroups) {
      stageIdToPendingTaskGroups.compute(stageId,
          new BiFunction<String, Deque<ScheduledTaskGroup>, Deque<ScheduledTaskGroup>>() {
            @Override
            public Deque<ScheduledTaskGroup> apply(final String s,
                                                   final Deque<ScheduledTaskGroup> scheduledTaskGroups) {
              if (scheduledTaskGroups == null) {
                final Deque<ScheduledTaskGroup> pendingTaskGroupsForStage = new ArrayDeque<>();
                pendingTaskGroupsForStage.add(scheduledTaskGroup);
                updateSchedulableStages(stageId, scheduledTaskGroup.getContainerType());
                return pendingTaskGroupsForStage;
              } else {
                scheduledTaskGroups.add(scheduledTaskGroup);
                return scheduledTaskGroups;
              }
            }
          });
    }
  }

  /**
   * Dequeues the next TaskGroup to be scheduled according to job dependency priority.
   * @return the next TaskGroup to be scheduled
   */
  @Override
  public Optional<ScheduledTaskGroup> dequeue() {
    ScheduledTaskGroup taskGroupToSchedule = null;
    final String stageId;
    try {
      stageId = schedulableStages.takeFirst();
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new SchedulingException(new Throwable("An exception occurred while trying to dequeue the next TaskGroup"));
    }

    synchronized (stageIdToPendingTaskGroups) {
      final Deque<ScheduledTaskGroup> pendingTaskGroupsForStage = stageIdToPendingTaskGroups.get(stageId);

      if (pendingTaskGroupsForStage == null) {
        schedulableStages.addLast(stageId);
      } else {
        taskGroupToSchedule = pendingTaskGroupsForStage.poll();
        if (pendingTaskGroupsForStage.isEmpty()) {
          stageIdToPendingTaskGroups.remove(stageId);
          stageIdToPendingTaskGroups.forEach((scheduledStageId, taskGroupList) ->
              updateSchedulableStages(scheduledStageId, taskGroupList.getFirst().getContainerType()));
        } else {
          schedulableStages.addLast(stageId);
        }
      }
    }

    return (taskGroupToSchedule == null) ? Optional.empty()
        : Optional.of(taskGroupToSchedule);
  }

  /**
   * Removes a stage and its descendant stages from this PQ.
   * @param stageId for the stage to begin the removal recursively.
   */
  @Override
  public void removeTaskGroupsAndDescendants(final String stageId) {
    synchronized (stageIdToPendingTaskGroups) {
      removeStageAndChildren(stageId);
    }
  }

  /**
   * Recursively removes a stage and its children stages from this PQ.
   * @param stageId for the stage to begin the removal recursively.
   */
  private void removeStageAndChildren(final String stageId) {
    schedulableStages.remove(stageId);
    stageIdToPendingTaskGroups.remove(stageId);

    physicalPlan.getStageDAG().getChildren(stageId).forEach(
        physicalStage -> removeStageAndChildren(physicalStage.getId()));
  }

  /**
   * Updates the two-level PQ by examining a new candidate stage.
   * If there are no stages with higher priority, the candidate can be made schedulable.
   *
   * NOTE: This method provides the "line up" between stages, by assigning priorities,
   * serving as the key to the "priority" implementation of this class.
   * @param candidateStageId for the stage that can potentially be scheduled.
   * @param candidateStageContainerType for the stage that can potentially be scheduled.
   */
  private void updateSchedulableStages(final String candidateStageId, final String candidateStageContainerType) {
    final DAG<PhysicalStage, PhysicalStageEdge> jobDAG = physicalPlan.getStageDAG();

    if (isSchedulable(candidateStageId, candidateStageContainerType)) {
      // Check for ancestor stages that became schedulable due to candidateStage's absence from the queue.
      jobDAG.getAncestors(candidateStageId).forEach(ancestorStage -> {
        if (schedulableStages.contains(ancestorStage.getId())) {
          // Remove the ancestor stage if it is of the same container type.
          if (candidateStageContainerType.equals(ancestorStage.getContainerType())) {
            schedulableStages.remove(ancestorStage.getId());
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
  private boolean isSchedulable(final String candidateStageId, final String candidateStageContainerType) {
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
  public void onJobScheduled(final PhysicalPlan physicalPlanForJob) {
    this.physicalPlan = physicalPlanForJob;
  }

  @Override
  public boolean isEmpty() {
    synchronized (stageIdToPendingTaskGroups) {
      for (final String stageId : schedulableStages) {
        if (!stageIdToPendingTaskGroups.get(stageId).isEmpty()) {
          return false;
        }
      }
      return true;
    }
  }

  @Override
  public void close() {
    schedulableStages.clear();
    stageIdToPendingTaskGroups.clear();
  }
}
