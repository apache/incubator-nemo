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

import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalStage;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalStageEdge;
import edu.snu.vortex.runtime.common.plan.physical.ScheduledTaskGroup;
import net.jcip.annotations.ThreadSafe;
import org.apache.reef.annotations.audience.DriverSide;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Keep tracks of all pending task groups.
 * This class provides two-level queue scheduling by prioritizing TaskGroups of parent stages to be scheduled first.
 * Stages that are mutually independent alternate turns in scheduling each of their TaskGroups.
 * When two stages are connected by a push edge, the child stage's priority is bumped up to match the parent's.
 * This PQ assumes that stages/task groups of higher priorities are never enqueued without first removing
 * those of lower priorities (which is how Scheduler behaves) for simplicity.
 */
@ThreadSafe
@DriverSide
public final class PendingTaskGroupPriorityQueue {
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
  public PendingTaskGroupPriorityQueue() {
    stageIdToPendingTaskGroups = new ConcurrentHashMap<>();
    schedulableStages = new LinkedBlockingDeque<>();
  }

  /**
   * Enqueues a TaskGroup to this PQ.
   * @param scheduledTaskGroup to enqueue.
   */
  public void enqueue(final ScheduledTaskGroup scheduledTaskGroup) {
    final String stageId = scheduledTaskGroup.getTaskGroup().getStageId();

    if (stageIdToPendingTaskGroups.containsKey(stageId)) {
      stageIdToPendingTaskGroups.get(stageId).addLast(scheduledTaskGroup);
    } else {
      final Deque<ScheduledTaskGroup> pendingTaskGroupsForStage = new ArrayDeque<>();
      pendingTaskGroupsForStage.add(scheduledTaskGroup);

      stageIdToPendingTaskGroups.put(stageId, pendingTaskGroupsForStage);
      updateSchedulableStages(stageId);
    }
  }

  /**
   * Dequeues the next TaskGroup to be scheduled according to job dependency priority.
   * @return the next TaskGroup to be scheduled
   * @throws InterruptedException can be thrown while trying to take a pending stage ID.
   */
  public ScheduledTaskGroup dequeueNextTaskGroup() throws InterruptedException {
    final String stageId = schedulableStages.takeFirst();

    final Deque<ScheduledTaskGroup> pendingTaskGroupsForStage = stageIdToPendingTaskGroups.get(stageId);
    final ScheduledTaskGroup taskGroupToSchedule = pendingTaskGroupsForStage.poll();

    if (pendingTaskGroupsForStage.isEmpty()) {
      stageIdToPendingTaskGroups.remove(stageId);
      stageIdToPendingTaskGroups.keySet().forEach(scheduledStageId -> updateSchedulableStages(scheduledStageId));
    } else {
      schedulableStages.addLast(stageId);
    }

    return taskGroupToSchedule;
  }

  /**
   * Removes a stage and its descendant stages from this PQ.
   * @param stageId for the stage to begin the removal recursively.
   */
  public void removeStageAndDescendantsFromQueue(final String stageId) {
    removeStageAndChildren(stageId);
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
   * @param candidateStageId for the stage that can potentially be scheduled.
   */
  private void updateSchedulableStages(final String candidateStageId) {
    boolean readyToScheduleImmediately = true;
    final DAG<PhysicalStage, PhysicalStageEdge> jobDAG = physicalPlan.getStageDAG();
    for (final PhysicalStage parentStage : jobDAG.getParents(candidateStageId)) {
      if (schedulableStages.contains(parentStage.getId())) {
        if ((jobDAG.getEdgeBetween(parentStage.getId(),
            candidateStageId).getAttributes().get(Attribute.Key.ChannelTransferPolicy) == Attribute.Pull)) {
          readyToScheduleImmediately = false;
          break;
        }
      }
    }

    if (readyToScheduleImmediately) {
      if (!schedulableStages.contains(candidateStageId)) {
        schedulableStages.addLast(candidateStageId);
      }
    }
  }

  public void onJobScheduled(final PhysicalPlan physicalPlanForJob) {
    this.physicalPlan = physicalPlanForJob;
  }
}
