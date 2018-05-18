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

import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.physical.*;
import edu.snu.nemo.runtime.plangenerator.TestPlanGenerator;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link SingleJobTaskGroupCollection}.
 */
public final class SingleTaskGroupQueueTest {
  private SingleJobTaskGroupCollection pendingTaskGroupPriorityQueue;

  /**
   * To be used for a thread pool to execute task groups.
   */
  private ExecutorService executorService;

  @Before
  public void setUp() throws Exception{
    pendingTaskGroupPriorityQueue = new SingleJobTaskGroupCollection();
    executorService = Executors.newFixedThreadPool(2);
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link SingleJobTaskGroupCollection}.
   * Tests whether the dequeued TaskGroups are according to the stage-dependency priority.
   */
  @Test
  public void testPushPriority() throws Exception {
    final PhysicalPlan physicalPlan =
        TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.ThreeSequentialVertices, true);

    pendingTaskGroupPriorityQueue.onJobScheduled(physicalPlan);
    final List<PhysicalStage> dagOf2Stages = physicalPlan.getStageDAG().getTopologicalSort();

    // Make sure that ScheduleGroups have been assigned to satisfy PendingPQ's requirements.
    assertEquals(dagOf2Stages.get(0).getScheduleGroupIndex(), dagOf2Stages.get(1).getScheduleGroupIndex());

    final CountDownLatch countDownLatch = new CountDownLatch(2);

    final AtomicBoolean passed = new AtomicBoolean(true);

    // This mimics Batch Scheduler's behavior
    executorService.submit(() -> {
      // First schedule the children TaskGroups (since it is push).
      // BatchSingleJobScheduler will schedule TaskGroups in this order as well.
      scheduleStage(dagOf2Stages.get(1));
      // Then, schedule the parent TaskGroups.
      scheduleStage(dagOf2Stages.get(0));

      countDownLatch.countDown();
    }).get();

    // This mimics SchedulerRunner's behavior
    executorService.submit(() -> {
      try {
        assertEquals(dequeueAndGetStageId(), dagOf2Stages.get(1).getId());
        final ScheduledTaskGroup dequeuedTaskGroup = dequeue();
        assertEquals(RuntimeIdGenerator.getStageIdFromTaskGroupId(dequeuedTaskGroup.getTaskGroupId()),
            dagOf2Stages.get(1).getId());

        // Let's say we fail to schedule, and add this TaskGroup back.
        pendingTaskGroupPriorityQueue.add(dequeuedTaskGroup);
        assertEquals(dequeueAndGetStageId(), dagOf2Stages.get(1).getId());

        // Now that we've dequeued all of the children TaskGroups, we should now start getting the parents.
        assertEquals(dequeueAndGetStageId(), dagOf2Stages.get(0).getId());
        assertEquals(dequeueAndGetStageId(), dagOf2Stages.get(0).getId());
      } catch (Exception e) {
        e.printStackTrace();
        passed.getAndSet(false);
      } finally {
        countDownLatch.countDown();
      }
    }).get();

    countDownLatch.await();
    assertTrue(passed.get());
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link SingleJobTaskGroupCollection}.
   * Tests whether the dequeued TaskGroups are according to the stage-dependency priority.
   */
  @Test
  public void testPullPriority() throws Exception {
    final PhysicalPlan physicalPlan =
        TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.ThreeSequentialVertices, false);
    pendingTaskGroupPriorityQueue.onJobScheduled(physicalPlan);
    final List<PhysicalStage> dagOf2Stages = physicalPlan.getStageDAG().getTopologicalSort();

    // Make sure that ScheduleGroups have been assigned to satisfy PendingPQ's requirements.
    assertEquals(dagOf2Stages.get(0).getScheduleGroupIndex(), 0);
    assertEquals(dagOf2Stages.get(1).getScheduleGroupIndex(), 1);

    final CountDownLatch countDownLatch = new CountDownLatch(2);

    // This mimics Batch Scheduler's behavior
    executorService.submit(() -> {
      // First schedule the parent TaskGroups (since it is pull).
      // BatchSingleJobScheduler will schedule TaskGroups in this order as well.
      scheduleStage(dagOf2Stages.get(0));
      countDownLatch.countDown();
    }).get();

    // This mimics SchedulerRunner's behavior
    executorService.submit(() -> {
      try {
        assertEquals(dequeueAndGetStageId(), dagOf2Stages.get(0).getId());
        final ScheduledTaskGroup dequeuedTaskGroup = dequeue();
        assertEquals(RuntimeIdGenerator.getStageIdFromTaskGroupId(dequeuedTaskGroup.getTaskGroupId()),
            dagOf2Stages.get(0).getId());

        // Let's say we fail to schedule, and add this TaskGroup back.
        pendingTaskGroupPriorityQueue.add(dequeuedTaskGroup);
        assertEquals(dequeueAndGetStageId(), dagOf2Stages.get(0).getId());

        // Now that we've dequeued all of the children TaskGroups, we should now schedule children.
        assertEquals(dequeueAndGetStageId(), dagOf2Stages.get(0).getId());

        // Schedule the children TaskGroups.
        scheduleStage(dagOf2Stages.get(1));
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        countDownLatch.countDown();
      }
    }).get();

    countDownLatch.await();
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link SingleJobTaskGroupCollection}.
   * Tests whether the dequeued TaskGroups are according to the stage-dependency priority,
   * while concurrently scheduling TaskGroups that have dependencies, but are of different container types.
   */
  @Test
  public void testWithDifferentContainerType() throws Exception {
    final PhysicalPlan physicalPlan = TestPlanGenerator.generatePhysicalPlan(
        TestPlanGenerator.PlanType.ThreeSequentialVerticesWithDifferentContainerTypes, true);
    pendingTaskGroupPriorityQueue.onJobScheduled(physicalPlan);
    final List<PhysicalStage> dagOf2Stages = physicalPlan.getStageDAG().getTopologicalSort();

    // Make sure that ScheduleGroups have been assigned to satisfy PendingPQ's requirements.
    assertEquals(dagOf2Stages.get(0).getScheduleGroupIndex(), dagOf2Stages.get(1).getScheduleGroupIndex());

    final CountDownLatch countDownLatch = new CountDownLatch(2);

    // First schedule the children TaskGroups (since it is push).
    // BatchSingleJobScheduler will schedule TaskGroups in this order as well.
    scheduleStage(dagOf2Stages.get(1));
    // Then, schedule the parent TaskGroups.
    scheduleStage(dagOf2Stages.get(0));

    countDownLatch.countDown();

    // This mimics SchedulerRunner's behavior.
    executorService.submit(() -> {
      try {
        assertEquals(dequeueAndGetStageId(), dagOf2Stages.get(1).getId());

        assertEquals(dequeueAndGetStageId(), dagOf2Stages.get(1).getId());

        assertEquals(dequeueAndGetStageId(), dagOf2Stages.get(0).getId());

        assertEquals(dequeueAndGetStageId(), dagOf2Stages.get(0).getId());

        assertEquals(dequeueAndGetStageId(), dagOf2Stages.get(0).getId());
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        countDownLatch.countDown();
      }
    }).get();

    countDownLatch.await();
  }

  /**
   * Schedule the task groups in a physical stage.
   * @param stage the stage to schedule.
   */
  private void scheduleStage(final PhysicalStage stage) {
    stage.getTaskGroupIds().forEach(taskGroupId ->
        pendingTaskGroupPriorityQueue.add(new ScheduledTaskGroup(
            "TestPlan", stage.getSerializedTaskGroupDag(), taskGroupId, Collections.emptyList(),
            Collections.emptyList(), 0, stage.getContainerType(), Collections.emptyMap())));
  }

  /**
   * Dequeues a scheduled task group from the task group priority queue and get it's stage name.
   * @return the stage name of the dequeued task group.
   */
  private String dequeueAndGetStageId() {
    final ScheduledTaskGroup scheduledTaskGroup = dequeue();
    return RuntimeIdGenerator.getStageIdFromTaskGroupId(scheduledTaskGroup.getTaskGroupId());
  }

  /**
   * Dequeues a scheduled task group from the task group priority queue.
   * @return the TaskGroup dequeued
   */
  private ScheduledTaskGroup dequeue() {
    final Collection<ScheduledTaskGroup> scheduledTaskGroups
        = pendingTaskGroupPriorityQueue.peekSchedulableTaskGroups().get();
    return pendingTaskGroupPriorityQueue.remove(scheduledTaskGroups.iterator().next().getTaskGroupId());
  }
}
