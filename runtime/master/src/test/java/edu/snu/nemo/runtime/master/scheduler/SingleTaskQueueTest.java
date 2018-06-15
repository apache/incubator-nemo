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

import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.Task;
import edu.snu.nemo.runtime.common.plan.PhysicalPlan;
import edu.snu.nemo.runtime.common.plan.Stage;
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
 * Tests {@link SingleJobTaskCollection}.
 */
public final class SingleTaskQueueTest {
  private SingleJobTaskCollection pendingTaskPriorityQueue;

  /**
   * To be used for a thread pool to execute tasks.
   */
  private ExecutorService executorService;

  @Before
  public void setUp() throws Exception{
    pendingTaskPriorityQueue = new SingleJobTaskCollection();
    executorService = Executors.newFixedThreadPool(2);
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link SingleJobTaskCollection}.
   * Tests whether the dequeued Tasks are according to the stage-dependency priority.
   */
  @Test
  public void testPushPriority() throws Exception {
    final PhysicalPlan physicalPlan =
        TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.ThreeSequentialVertices, true);

    pendingTaskPriorityQueue.onJobScheduled(physicalPlan);
    final List<Stage> dagOf2Stages = physicalPlan.getStageDAG().getTopologicalSort();

    // Make sure that ScheduleGroups have been assigned to satisfy PendingPQ's requirements.
    assertEquals(dagOf2Stages.get(0).getScheduleGroupIndex(), dagOf2Stages.get(1).getScheduleGroupIndex());

    final CountDownLatch countDownLatch = new CountDownLatch(2);

    final AtomicBoolean passed = new AtomicBoolean(true);

    // This mimics Batch Scheduler's behavior
    executorService.submit(() -> {
      // First schedule the children Tasks (since it is push).
      // BatchSingleJobScheduler will schedule Tasks in this order as well.
      scheduleStage(dagOf2Stages.get(1));
      // Then, schedule the parent Tasks.
      scheduleStage(dagOf2Stages.get(0));

      countDownLatch.countDown();
    }).get();

    // This mimics SchedulerRunner's behavior
    executorService.submit(() -> {
      try {
        assertEquals(dequeueAndGetStageId(), dagOf2Stages.get(1).getId());
        final Task dequeuedTask = dequeue();
        assertEquals(RuntimeIdGenerator.getStageIdFromTaskId(dequeuedTask.getTaskId()),
            dagOf2Stages.get(1).getId());

        // Let's say we fail to schedule, and add this Task back.
        pendingTaskPriorityQueue.add(dequeuedTask);
        assertEquals(dequeueAndGetStageId(), dagOf2Stages.get(1).getId());

        // Now that we've dequeued all of the children Tasks, we should now start getting the parents.
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
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link SingleJobTaskCollection}.
   * Tests whether the dequeued Tasks are according to the stage-dependency priority.
   */
  @Test
  public void testPullPriority() throws Exception {
    final PhysicalPlan physicalPlan =
        TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.ThreeSequentialVertices, false);
    pendingTaskPriorityQueue.onJobScheduled(physicalPlan);
    final List<Stage> dagOf2Stages = physicalPlan.getStageDAG().getTopologicalSort();

    // Make sure that ScheduleGroups have been assigned to satisfy PendingPQ's requirements.
    assertEquals(dagOf2Stages.get(0).getScheduleGroupIndex(), 0);
    assertEquals(dagOf2Stages.get(1).getScheduleGroupIndex(), 1);

    final CountDownLatch countDownLatch = new CountDownLatch(2);

    // This mimics Batch Scheduler's behavior
    executorService.submit(() -> {
      // First schedule the parent Tasks (since it is pull).
      // BatchSingleJobScheduler will schedule Tasks in this order as well.
      scheduleStage(dagOf2Stages.get(0));
      countDownLatch.countDown();
    }).get();

    // This mimics SchedulerRunner's behavior
    executorService.submit(() -> {
      try {
        assertEquals(dequeueAndGetStageId(), dagOf2Stages.get(0).getId());
        final Task dequeuedTask = dequeue();
        assertEquals(RuntimeIdGenerator.getStageIdFromTaskId(dequeuedTask.getTaskId()),
            dagOf2Stages.get(0).getId());

        // Let's say we fail to schedule, and add this Task back.
        pendingTaskPriorityQueue.add(dequeuedTask);
        assertEquals(dequeueAndGetStageId(), dagOf2Stages.get(0).getId());

        // Now that we've dequeued all of the children Tasks, we should now schedule children.
        assertEquals(dequeueAndGetStageId(), dagOf2Stages.get(0).getId());

        // Schedule the children Tasks.
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
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link SingleJobTaskCollection}.
   * Tests whether the dequeued Tasks are according to the stage-dependency priority,
   * while concurrently scheduling Tasks that have dependencies, but are of different container types.
   */
  @Test
  public void testWithDifferentContainerType() throws Exception {
    final PhysicalPlan physicalPlan = TestPlanGenerator.generatePhysicalPlan(
        TestPlanGenerator.PlanType.ThreeSequentialVerticesWithDifferentContainerTypes, true);
    pendingTaskPriorityQueue.onJobScheduled(physicalPlan);
    final List<Stage> dagOf2Stages = physicalPlan.getStageDAG().getTopologicalSort();

    // Make sure that ScheduleGroups have been assigned to satisfy PendingPQ's requirements.
    assertEquals(dagOf2Stages.get(0).getScheduleGroupIndex(), dagOf2Stages.get(1).getScheduleGroupIndex());

    final CountDownLatch countDownLatch = new CountDownLatch(2);

    // First schedule the children Tasks (since it is push).
    // BatchSingleJobScheduler will schedule Tasks in this order as well.
    scheduleStage(dagOf2Stages.get(1));
    // Then, schedule the parent Tasks.
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
   * Schedule the tasks in a stage.
   * @param stage the stage to schedule.
   */
  private void scheduleStage(final Stage stage) {
    stage.getTaskIds().forEach(taskId ->
        pendingTaskPriorityQueue.add(new Task(
            "TestPlan",
            taskId,
            0,
            stage.getContainerType(),
            stage.getSerializedIRDAG(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyMap())));
  }

  /**
   * Dequeues a scheduled task from the task priority queue and get it's stage name.
   * @return the stage name of the dequeued task.
   */
  private String dequeueAndGetStageId() {
    final Task task = dequeue();
    return RuntimeIdGenerator.getStageIdFromTaskId(task.getTaskId());
  }

  /**
   * Dequeues a scheduled task from the task priority queue.
   * @return the Task dequeued
   */
  private Task dequeue() {
    final Collection<Task> tasks
        = pendingTaskPriorityQueue.peekSchedulableStage().get();
    return pendingTaskPriorityQueue.remove(tasks.iterator().next().getTaskId());
  }
}
