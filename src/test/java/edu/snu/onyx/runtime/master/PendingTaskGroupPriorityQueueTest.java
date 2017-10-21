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
package edu.snu.onyx.runtime.master;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.dag.DAGBuilder;
import edu.snu.onyx.compiler.frontend.beam.transform.DoTransform;
import edu.snu.onyx.compiler.ir.IREdge;
import edu.snu.onyx.compiler.ir.IRVertex;
import edu.snu.onyx.compiler.ir.OperatorVertex;
import edu.snu.onyx.compiler.ir.Transform;
import edu.snu.onyx.compiler.ir.executionproperty.vertex.ExecutorPlacementProperty;
import edu.snu.onyx.compiler.ir.executionproperty.vertex.ParallelismProperty;
import edu.snu.onyx.compiler.optimizer.Optimizer;
import edu.snu.onyx.compiler.optimizer.TestPolicy;
import edu.snu.onyx.runtime.common.plan.physical.*;
import edu.snu.onyx.runtime.executor.datatransfer.communication.OneToOne;
import edu.snu.onyx.runtime.executor.datatransfer.communication.ScatterGather;
import edu.snu.onyx.runtime.master.scheduler.*;
import org.apache.reef.tang.Tang;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link PendingTaskGroupPriorityQueue}.
 */
public final class PendingTaskGroupPriorityQueueTest {
  private DAGBuilder<IRVertex, IREdge> irDAGBuilder;
  private PendingTaskGroupPriorityQueue pendingTaskGroupPriorityQueue;

  /**
   * To be used for a thread pool to execute task groups.
   */
  private ExecutorService executorService;

  @Before
  public void setUp() {
    irDAGBuilder = new DAGBuilder<>();
    pendingTaskGroupPriorityQueue = new PendingTaskGroupPriorityQueue();
    executorService = Executors.newFixedThreadPool(2);
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link PendingTaskGroupPriorityQueue}.
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

    final IREdge e1 = new IREdge(ScatterGather.class, v1, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(OneToOne.class, v2, v3, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e2);

    final DAG<IRVertex, IREdge> irDAG = Optimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(),
        new TestPolicy(true), "");
    final PhysicalPlanGenerator physicalPlanGenerator =
        Tang.Factory.getTang().newInjector().getInstance(PhysicalPlanGenerator.class);
    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = irDAG.convert(physicalPlanGenerator);

    pendingTaskGroupPriorityQueue.onJobScheduled(
        new PhysicalPlan("TestPlan", physicalDAG, physicalPlanGenerator.getTaskIRVertexMap()));

    final List<PhysicalStage> dagOf2Stages = physicalDAG.getTopologicalSort();

    // Make sure that ScheduleGroups have been assigned to satisfy PendingPQ's requirements.
    assertEquals(dagOf2Stages.get(0).getScheduleGroupIndex(), dagOf2Stages.get(1).getScheduleGroupIndex());

    // This mimics Batch Scheduler's behavior
    executorService.execute(() -> {
          // First schedule the children TaskGroups (since it is push).
          // BatchScheduler will schedule TaskGroups in this order as well.
          dagOf2Stages.get(1).getTaskGroupList().forEach(taskGroup ->
              pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup(taskGroup, null, null, 0)));

          // Then, schedule the parent TaskGroups.
          dagOf2Stages.get(0).getTaskGroupList().forEach(taskGroup ->
              pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup(taskGroup, null, null, 0)));
        });

    // This mimics SchedulerRunner's behavior
    executorService.execute(() -> {
      try {
        assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(1).getId());
        final ScheduledTaskGroup dequeuedTaskGroup = pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get();
        assertEquals(dequeuedTaskGroup.getTaskGroup().getStageId(), dagOf2Stages.get(1).getId());

        // Let's say we fail to schedule, and enqueue this TaskGroup back.
        pendingTaskGroupPriorityQueue.enqueue(dequeuedTaskGroup);
        assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(1).getId());

        // Now that we've dequeued all of the children TaskGroups, we should now start getting the parents.
        assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(0).getId());
        assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(0).getId());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }});
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link PendingTaskGroupPriorityQueue}.
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

    final IREdge e1 = new IREdge(ScatterGather.class, v1, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(OneToOne.class, v2, v3, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e2);

    final DAG<IRVertex, IREdge> irDAG = Optimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(),
        new TestPolicy(), "");
    final PhysicalPlanGenerator physicalPlanGenerator =
        Tang.Factory.getTang().newInjector().getInstance(PhysicalPlanGenerator.class);
    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = irDAG.convert(physicalPlanGenerator);

    pendingTaskGroupPriorityQueue.onJobScheduled(
        new PhysicalPlan("TestPlan", physicalDAG, physicalPlanGenerator.getTaskIRVertexMap()));

    final List<PhysicalStage> dagOf2Stages = physicalDAG.getTopologicalSort();

    // Make sure that ScheduleGroups have been assigned to satisfy PendingPQ's requirements.
    assertEquals(dagOf2Stages.get(0).getScheduleGroupIndex(), 0);
    assertEquals(dagOf2Stages.get(1).getScheduleGroupIndex(), 1);

    // This mimics Batch Scheduler's behavior
    executorService.execute(() -> {
      // First schedule the parent TaskGroups (since it is pull).
      // BatchScheduler will schedule TaskGroups in this order as well.
      dagOf2Stages.get(0).getTaskGroupList().forEach(taskGroup ->
          pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup(taskGroup, null, null, 0)));
    });

    // This mimics SchedulerRunner's behavior
    executorService.execute(() -> {
      try {
        assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(0).getId());
        final ScheduledTaskGroup dequeuedTaskGroup = pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get();
        assertEquals(dequeuedTaskGroup.getTaskGroup().getStageId(), dagOf2Stages.get(0).getId());

        // Let's say we fail to schedule, and enqueue this TaskGroup back.
        pendingTaskGroupPriorityQueue.enqueue(dequeuedTaskGroup);
        assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(0).getId());

        // Now that we've dequeued all of the children TaskGroups, we should now schedule children.
        assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(0).getId());
        assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(0).getId());

        // Schedule the children TaskGroups.
        dagOf2Stages.get(1).getTaskGroupList().forEach(taskGroup ->
            pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup(taskGroup, null, null, 0)));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }});
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link PendingTaskGroupPriorityQueue}.
   * Tests whether the dequeue                                                                                          d TaskGroups are according to the stage-dependency priority.
   */
  @Test
  public void test2RootStageDependency() throws Exception {
    final Transform t = mock(Transform.class);
    final IRVertex v1 = new OperatorVertex(t);
    v1.setProperty(ParallelismProperty.of(3));
    v1.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v1);

    final IRVertex v2 = new OperatorVertex(t);
    v2.setProperty(ParallelismProperty.of(2));
    v2.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v2);

    final IRVertex v3 = new OperatorVertex(new DoTransform(null, null));
    v3.setProperty(ParallelismProperty.of(4));
    v3.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v3);

    final IREdge e1 = new IREdge(ScatterGather.class, v1, v3, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(ScatterGather.class, v2, v3, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e2);

    final DAG<IRVertex, IREdge> irDAG = Optimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(),
        new TestPolicy(), "");
    final PhysicalPlanGenerator physicalPlanGenerator =
        Tang.Factory.getTang().newInjector().getInstance(PhysicalPlanGenerator.class);
    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = irDAG.convert(physicalPlanGenerator);

    pendingTaskGroupPriorityQueue.onJobScheduled(
        new PhysicalPlan("TestPlan", physicalDAG, physicalPlanGenerator.getTaskIRVertexMap()));

    final List<PhysicalStage> dagOf3Stages = physicalDAG.getTopologicalSort();

    // Add TaskGroups of the first stage to the queue.
    dagOf3Stages.get(0).getTaskGroupList().forEach(taskGroup ->
        pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup(taskGroup, null, null, 0)));

    // Add TaskGroups of the second stage to the queue (this is the earliest point 2nd stage can be scheduled)
    dagOf3Stages.get(1).getTaskGroupList().forEach(taskGroup ->
        pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup(taskGroup, null, null, 0)));

    // The 1st dequeued TaskGroup should belong to the 1st stage.
    assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
        dagOf3Stages.get(0).getId());

    // The 2nd dequeued TaskGroup should belong to the 2nd stage.
    assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
        dagOf3Stages.get(1).getId());

    // The 3rd dequeued TaskGroup should also belong to the 1st stage.
    assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
        dagOf3Stages.get(0).getId());

    // The 4th dequeued TaskGroup should belong to the 2nd stage.
    ScheduledTaskGroup dequeued = pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get();
    assertEquals(dequeued.getTaskGroup().getStageId(), dagOf3Stages.get(1).getId());

    pendingTaskGroupPriorityQueue.enqueue(dequeued);

    // The 5th dequeued TaskGroup should belong to the 1st stage.
    assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
        dagOf3Stages.get(0).getId());

    // The 6th dequeued TaskGroup should again belong to the 2nd stage.
    assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
        dagOf3Stages.get(1).getId());

    // Add TaskGroups of the third stage to the queue. (this is the earliest point 3rd stage can be scheduled)
    dagOf3Stages.get(2).getTaskGroupList().forEach(taskGroup ->
        pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup(taskGroup, null, null, 0)));

    // The dequeued TaskGroup should belong to the 3rd stage.
    assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
        dagOf3Stages.get(2).getId());
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link PendingTaskGroupPriorityQueue}.
   * Tests whether the dequeued TaskGroups are according to the stage-dependency priority.
   */
  @Test
  public void testStageDependencyRemoval() throws Exception {
    final Transform t = mock(Transform.class);
    final IRVertex v1 = new OperatorVertex(t);
    v1.setProperty(ParallelismProperty.of(3));
    v1.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v1);

    final IRVertex v2 = new OperatorVertex(t);
    v2.setProperty(ParallelismProperty.of(2));
    v2.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v2);

    final IRVertex v3 = new OperatorVertex(new DoTransform(null, null));
    v3.setProperty(ParallelismProperty.of(4));
    v3.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v3);

    final IREdge e1 = new IREdge(ScatterGather.class, v1, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(ScatterGather.class, v2, v3, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e2);

    final DAG<IRVertex, IREdge> irDAG = Optimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(),
          new TestPolicy(), "");
    final PhysicalPlanGenerator physicalPlanGenerator =
        Tang.Factory.getTang().newInjector().getInstance(PhysicalPlanGenerator.class);
    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = irDAG.convert(physicalPlanGenerator);

    pendingTaskGroupPriorityQueue.onJobScheduled(
        new PhysicalPlan("TestPlan", physicalDAG, physicalPlanGenerator.getTaskIRVertexMap()));

    final List<PhysicalStage> dagOf3Stages = physicalDAG.getTopologicalSort();

    // Add TaskGroups of the first stage to the queue.
    dagOf3Stages.get(0).getTaskGroupList().forEach(taskGroup ->
        pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup(taskGroup, null, null, 0)));

    // The 1st dequeued TaskGroup should belong to the 1st stage.
    assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
        dagOf3Stages.get(0).getId());

    // The 2nd dequeued TaskGroup should also belong to the 1st stage.
    assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
        dagOf3Stages.get(0).getId());

    // The 3rd dequeued TaskGroup should also belong to the 1st stage.
    ScheduledTaskGroup dequeued = pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get();
    assertEquals(dequeued.getTaskGroup().getStageId(), dagOf3Stages.get(0).getId());

    // Add TaskGroups of the second stage to the queue (this is the earliest point 2nd stage can be scheduled)
    dagOf3Stages.get(1).getTaskGroupList().forEach(taskGroup ->
        pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup(taskGroup, null, null, 0)));

    // Remove the 2nd stage from dependencies.
    pendingTaskGroupPriorityQueue.removeStageAndDescendantsFromQueue(dagOf3Stages.get(1).getId());

    // Put the TaskGroup from the 1st stage back into the queue.
    pendingTaskGroupPriorityQueue.enqueue(dequeued);

    // The dequeued TaskGroup should again belong to the 1st stage.
    dequeued = pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get();
    assertEquals(dequeued.getTaskGroup().getStageId(), dagOf3Stages.get(0).getId());

    // Add TaskGroups of the second stage to the queue (this is the earliest point 2nd stage can be scheduled)
    dagOf3Stages.get(1).getTaskGroupList().forEach(taskGroup ->
        pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup(taskGroup, null, null, 0)));

    // The dequeued TaskGroup should belong to the 2nd stage.
    assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
        dagOf3Stages.get(1).getId());

    // The 2nd dequeued TaskGroup should also belong to the 2nd stage.
    assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
        dagOf3Stages.get(1).getId());

    // Add TaskGroups of the third stage to the queue. (this is the earliest point 3rd stage can be scheduled)
    dagOf3Stages.get(2).getTaskGroupList().forEach(taskGroup ->
        pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup(taskGroup, null, null, 0)));

    // The dequeued TaskGroup should belong to the 3rd stage.
    assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
        dagOf3Stages.get(2).getId());
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link PendingTaskGroupPriorityQueue}.
   * Tests whether the dequeued TaskGroups are according to the stage-dependency priority,
   * while concurrently scheduling TaskGroups that have dependencies, but are of different container types.
   */
  @Test
  public void testContainerTypeAwareness() throws Exception {
    final Transform t = mock(Transform.class);
    final IRVertex v1 = new OperatorVertex(t);
    v1.setProperty(ParallelismProperty.of(3));
    v1.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v1);

    final IRVertex v2 = new OperatorVertex(t);
    v2.setProperty(ParallelismProperty.of(2));
    v2.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.TRANSIENT));
    irDAGBuilder.addVertex(v2);

    final IRVertex v3 = new OperatorVertex(new DoTransform(null, null));
    v3.setProperty(ParallelismProperty.of(4));
    v3.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.RESERVED));
    irDAGBuilder.addVertex(v3);

    final IREdge e1 = new IREdge(ScatterGather.class, v1, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(ScatterGather.class, v2, v3, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e2);

    final DAG<IRVertex, IREdge> irDAG = Optimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(),
        new TestPolicy(), "");
    final PhysicalPlanGenerator physicalPlanGenerator =
        Tang.Factory.getTang().newInjector().getInstance(PhysicalPlanGenerator.class);
    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = irDAG.convert(physicalPlanGenerator);

    pendingTaskGroupPriorityQueue.onJobScheduled(
        new PhysicalPlan("TestPlan", physicalDAG, physicalPlanGenerator.getTaskIRVertexMap()));

    final List<PhysicalStage> dagOf3Stages = physicalDAG.getTopologicalSort();
    for (int i = 0; i < dagOf3Stages.size(); i++) {
      dagOf3Stages.get(i).getTaskGroupList().forEach(taskGroup ->
          pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup(taskGroup, null, null, 0)));
    }

    assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
        dagOf3Stages.get(0).getId());
    assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
        dagOf3Stages.get(1).getId());
    assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
        dagOf3Stages.get(2).getId());
  }
}
