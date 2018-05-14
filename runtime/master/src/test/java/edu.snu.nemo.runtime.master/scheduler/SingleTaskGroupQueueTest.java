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

import edu.snu.nemo.common.coder.Coder;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.physical.*;
import edu.snu.nemo.runtime.master.physicalplans.TestPlanGenerator;
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
import static org.mockito.Mockito.mock;

/**
 * Tests {@link SingleJobTaskGroupCollection}.
 */
public final class SingleTaskGroupQueueTest {
  private DAGBuilder<IRVertex, IREdge> irDAGBuilder;
  private SingleJobTaskGroupCollection pendingTaskGroupPriorityQueue;

  /**
   * To be used for a thread pool to execute task groups.
   */
  private ExecutorService executorService;

  @Before
  public void setUp() throws Exception{
    irDAGBuilder = new DAGBuilder<>();
    pendingTaskGroupPriorityQueue = new SingleJobTaskGroupCollection();
    executorService = Executors.newFixedThreadPool(2);
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link SingleJobTaskGroupCollection}.
   * Tests whether the dequeued TaskGroups are according to the stage-dependency priority.
   */
  @Test
  public void testPushPriority() throws Exception {
    final Transform t = mock(Transform.class);
    final IRVertex v1 = new OperatorVertex(t);
    v1.setProperty(ParallelismProperty.of(3));
    v1.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v1);

    final IRVertex v2 = new OperatorVertex(t);
    v2.setProperty(ParallelismProperty.of(2));
    v2.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v2);

    final IRVertex v3 = new OperatorVertex(t);
    v3.setProperty(ParallelismProperty.of(2));
    v3.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v3);

    final IREdge e1 = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, v1, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(DataCommunicationPatternProperty.Value.OneToOne, v2, v3, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e2);

    final PhysicalPlan physicalPlan = TestPlanGenerator.getSimplePullPlan();
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
    final Transform t = mock(Transform.class);
    final IRVertex v1 = new OperatorVertex(t);
    v1.setProperty(ParallelismProperty.of(3));
    v1.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v1);

    final IRVertex v2 = new OperatorVertex(t);
    v2.setProperty(ParallelismProperty.of(2));
    v2.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v2);

    final IRVertex v3 = new OperatorVertex(t);
    v3.setProperty(ParallelismProperty.of(2));
    v3.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v3);

    final IREdge e1 = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, v1, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(DataCommunicationPatternProperty.Value.OneToOne, v2, v3, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e2);

    final PhysicalPlan physicalPlan = TestPlanGenerator.getSimplePullPlan();
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
    final Transform t = mock(Transform.class);
    final IRVertex v1 = new OperatorVertex(t);
    v1.setProperty(ParallelismProperty.of(3));
    v1.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v1);

    final IRVertex v2 = new OperatorVertex(t);
    v2.setProperty(ParallelismProperty.of(2));
    v2.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.TRANSIENT));
    irDAGBuilder.addVertex(v2);

    final IRVertex v3 = new OperatorVertex(t);
    v3.setProperty(ParallelismProperty.of(2));
    v3.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.TRANSIENT));
    irDAGBuilder.addVertex(v3);

    final IREdge e1 = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, v1, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(DataCommunicationPatternProperty.Value.OneToOne, v2, v3, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e2);

    final PhysicalPlan physicalPlan = TestPlanGenerator.getSimplePullPlan();
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
