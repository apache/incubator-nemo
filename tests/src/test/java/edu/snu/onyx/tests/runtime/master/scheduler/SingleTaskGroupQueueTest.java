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
package edu.snu.onyx.tests.runtime.master.scheduler;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.dag.DAGBuilder;
import edu.snu.onyx.common.ir.vertex.transform.Transform;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.ir.vertex.OperatorVertex;
import edu.snu.onyx.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.onyx.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.onyx.compiler.optimizer.CompiletimeOptimizer;
import edu.snu.onyx.conf.JobConf;
import edu.snu.onyx.runtime.common.plan.physical.*;
import edu.snu.onyx.runtime.master.scheduler.SingleJobTaskGroupQueue;
import edu.snu.onyx.tests.compiler.optimizer.TestPolicy;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link SingleJobTaskGroupQueue}.
 */
public final class SingleTaskGroupQueueTest {
  private DAGBuilder<IRVertex, IREdge> irDAGBuilder;
  private SingleJobTaskGroupQueue pendingTaskGroupPriorityQueue;
  private PhysicalPlanGenerator physicalPlanGenerator;

  /**
   * To be used for a thread pool to execute task groups.
   */
  private ExecutorService executorService;

  @Before
  public void setUp() throws Exception{
    irDAGBuilder = new DAGBuilder<>();
    pendingTaskGroupPriorityQueue = new SingleJobTaskGroupQueue();
    executorService = Executors.newFixedThreadPool(2);

    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(JobConf.DAGDirectory.class, "");
    physicalPlanGenerator = injector.getInstance(PhysicalPlanGenerator.class);
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link SingleJobTaskGroupQueue}.
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

    final DAG<IRVertex, IREdge> irDAG = CompiletimeOptimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(),
        new TestPolicy(true), "");

    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = irDAG.convert(physicalPlanGenerator);

    pendingTaskGroupPriorityQueue.onJobScheduled(
        new PhysicalPlan("TestPlan", physicalDAG, physicalPlanGenerator.getTaskIRVertexMap()));

    final List<PhysicalStage> dagOf2Stages = physicalDAG.getTopologicalSort();

    // Make sure that ScheduleGroups have been assigned to satisfy PendingPQ's requirements.
    assertEquals(dagOf2Stages.get(0).getScheduleGroupIndex(), dagOf2Stages.get(1).getScheduleGroupIndex());

    final CountDownLatch countDownLatch = new CountDownLatch(2);

    final AtomicBoolean passed = new AtomicBoolean(true);

    // This mimics Batch Scheduler's behavior
    executorService.execute(() -> {
      // First schedule the children TaskGroups (since it is push).
      // BatchSingleJobScheduler will schedule TaskGroups in this order as well.
      dagOf2Stages.get(1).getTaskGroupList().forEach(taskGroup ->
          pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup("TestPlan", taskGroup, null, null, 0)));

      // Then, schedule the parent TaskGroups.
      dagOf2Stages.get(0).getTaskGroupList().forEach(taskGroup ->
          pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup("TestPlan", taskGroup, null, null, 0)));

      countDownLatch.countDown();
    });

    // This mimics SchedulerRunner's behavior
    executorService.execute(() -> {
      try {
        assertEquals(pendingTaskGroupPriorityQueue.dequeue().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(1).getId());
        final ScheduledTaskGroup dequeuedTaskGroup = pendingTaskGroupPriorityQueue.dequeue().get();
        assertEquals(dequeuedTaskGroup.getTaskGroup().getStageId(), dagOf2Stages.get(1).getId());

        // Let's say we fail to schedule, and enqueue this TaskGroup back.
        pendingTaskGroupPriorityQueue.enqueue(dequeuedTaskGroup);
        assertEquals(pendingTaskGroupPriorityQueue.dequeue().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(1).getId());

        // Now that we've dequeued all of the children TaskGroups, we should now start getting the parents.
        assertEquals(pendingTaskGroupPriorityQueue.dequeue().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(0).getId());
        assertEquals(pendingTaskGroupPriorityQueue.dequeue().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(0).getId());
      } catch (Exception e) {
        e.printStackTrace();
        passed.getAndSet(false);
      } finally {
        countDownLatch.countDown();
      }
    });

    countDownLatch.await();
    assertTrue(passed.get());
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link SingleJobTaskGroupQueue}.
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

    final DAG<IRVertex, IREdge> irDAG = CompiletimeOptimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(),
        new TestPolicy(), "");

    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = irDAG.convert(physicalPlanGenerator);

    pendingTaskGroupPriorityQueue.onJobScheduled(
        new PhysicalPlan("TestPlan", physicalDAG, physicalPlanGenerator.getTaskIRVertexMap()));

    final List<PhysicalStage> dagOf2Stages = physicalDAG.getTopologicalSort();

    // Make sure that ScheduleGroups have been assigned to satisfy PendingPQ's requirements.
    assertEquals(dagOf2Stages.get(0).getScheduleGroupIndex(), 0);
    assertEquals(dagOf2Stages.get(1).getScheduleGroupIndex(), 1);

    final CountDownLatch countDownLatch = new CountDownLatch(2);

    // This mimics Batch Scheduler's behavior
    executorService.execute(() -> {
      // First schedule the parent TaskGroups (since it is pull).
      // BatchSingleJobScheduler will schedule TaskGroups in this order as well.
      dagOf2Stages.get(0).getTaskGroupList().forEach(taskGroup ->
          pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup("TestPlan", taskGroup, null, null, 0)));

      countDownLatch.countDown();
    });

    // This mimics SchedulerRunner's behavior
    executorService.execute(() -> {
      try {
        assertEquals(pendingTaskGroupPriorityQueue.dequeue().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(0).getId());
        final ScheduledTaskGroup dequeuedTaskGroup = pendingTaskGroupPriorityQueue.dequeue().get();
        assertEquals(dequeuedTaskGroup.getTaskGroup().getStageId(), dagOf2Stages.get(0).getId());

        // Let's say we fail to schedule, and enqueue this TaskGroup back.
        pendingTaskGroupPriorityQueue.enqueue(dequeuedTaskGroup);
        assertEquals(pendingTaskGroupPriorityQueue.dequeue().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(0).getId());

        // Now that we've dequeued all of the children TaskGroups, we should now schedule children.
        assertEquals(pendingTaskGroupPriorityQueue.dequeue().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(0).getId());

        // Schedule the children TaskGroups.
        dagOf2Stages.get(1).getTaskGroupList().forEach(taskGroup ->
          pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup("TestPlan", taskGroup, null, null, 0)));
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        countDownLatch.countDown();
      }
    });

    countDownLatch.await();
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link SingleJobTaskGroupQueue}.
   * Tests whether the dequeued TaskGroups are according to the stage-dependency priority.
   */
  @Test
  public void testPushRemoveAndAddStageDependency() throws Exception {
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

    final DAG<IRVertex, IREdge> irDAG = CompiletimeOptimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(),
        new TestPolicy(true), "");

    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = irDAG.convert(physicalPlanGenerator);

    pendingTaskGroupPriorityQueue.onJobScheduled(
        new PhysicalPlan("TestPlan", physicalDAG, physicalPlanGenerator.getTaskIRVertexMap()));

    final List<PhysicalStage> dagOf2Stages = physicalDAG.getTopologicalSort();

    // Make sure that ScheduleGroups have been assigned to satisfy PendingPQ's requirements.
    assertEquals(dagOf2Stages.get(0).getScheduleGroupIndex(), dagOf2Stages.get(1).getScheduleGroupIndex());

    final CountDownLatch countDownLatch = new CountDownLatch(2);

    // This mimics SchedulerRunner's behavior, but let's schedule this thread first this time,
    // as opposed to testPushPriority.
    executorService.execute(() -> {
      try {
        assertEquals(pendingTaskGroupPriorityQueue.dequeue().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(1).getId());
        final ScheduledTaskGroup dequeuedTaskGroup = pendingTaskGroupPriorityQueue.dequeue().get();
        assertEquals(dequeuedTaskGroup.getTaskGroup().getStageId(), dagOf2Stages.get(1).getId());

        // SchedulerRunner will never dequeue another TaskGroup before enquing back the failed TaskGroup,
        // but just for testing purposes of PendingTGPQ...
        assertEquals(pendingTaskGroupPriorityQueue.dequeue().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(0).getId());

        // Let's say we fail to schedule, and enqueue this TaskGroup back.
        pendingTaskGroupPriorityQueue.enqueue(dequeuedTaskGroup);
        assertEquals(pendingTaskGroupPriorityQueue.dequeue().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(1).getId());

        // Now that we've dequeued all of the children TaskGroups, we should now start getting the parents.
        assertEquals(pendingTaskGroupPriorityQueue.dequeue().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(0).getId());
        assertEquals(pendingTaskGroupPriorityQueue.dequeue().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(0).getId());
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        countDownLatch.countDown();
      }
    });

    // This mimics Batch Scheduler's behavior
    executorService.execute(() -> {
      // First schedule the children TaskGroups (since it is push).
      // BatchSingleJobScheduler will schedule TaskGroups in this order as well.
      dagOf2Stages.get(1).getTaskGroupList().forEach(taskGroup ->
          pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup("TestPlan", taskGroup, null, null, 0)));

      // Then, schedule the parent TaskGroups.
      dagOf2Stages.get(0).getTaskGroupList().forEach(taskGroup ->
          pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup("TestPlan", taskGroup, null, null, 0)));

      countDownLatch.countDown();
    });

    countDownLatch.await();
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link SingleJobTaskGroupQueue}.
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

    final IRVertex v3 = new OperatorVertex(t);
    v3.setProperty(ParallelismProperty.of(2));
    v3.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.TRANSIENT));
    irDAGBuilder.addVertex(v3);

    final IREdge e1 = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, v1, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(DataCommunicationPatternProperty.Value.OneToOne, v2, v3, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e2);

    final DAG<IRVertex, IREdge> irDAG = CompiletimeOptimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(),
        new TestPolicy(true), "");

    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = irDAG.convert(physicalPlanGenerator);

    pendingTaskGroupPriorityQueue.onJobScheduled(
        new PhysicalPlan("TestPlan", physicalDAG, physicalPlanGenerator.getTaskIRVertexMap()));

    final List<PhysicalStage> dagOf2Stages = physicalDAG.getTopologicalSort();

    // Make sure that ScheduleGroups have been assigned to satisfy PendingPQ's requirements.
    assertEquals(dagOf2Stages.get(0).getScheduleGroupIndex(), dagOf2Stages.get(1).getScheduleGroupIndex());

    final CountDownLatch countDownLatch = new CountDownLatch(2);

    // First schedule the children TaskGroups (since it is push).
    // BatchSingleJobScheduler will schedule TaskGroups in this order as well.
    dagOf2Stages.get(1).getTaskGroupList().forEach(taskGroup ->
        pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup("TestPlan", taskGroup, null, null, 0)));

    // Then, schedule the parent TaskGroups.
    dagOf2Stages.get(0).getTaskGroupList().forEach(taskGroup ->
        pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup("TestPlan", taskGroup, null, null, 0)));

    countDownLatch.countDown();

    // This mimics SchedulerRunner's behavior.
    executorService.execute(() -> {
      try {
        // Since Stage-0 and Stage-1 have different container types, they should simply alternate turns in scheduling.
        assertEquals(pendingTaskGroupPriorityQueue.dequeue().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(1).getId());

        assertEquals(pendingTaskGroupPriorityQueue.dequeue().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(0).getId());

        assertEquals(pendingTaskGroupPriorityQueue.dequeue().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(1).getId());

        assertEquals(pendingTaskGroupPriorityQueue.dequeue().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(0).getId());

        assertEquals(pendingTaskGroupPriorityQueue.dequeue().get().getTaskGroup().getStageId(),
            dagOf2Stages.get(0).getId());
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        countDownLatch.countDown();
      }
    });

    countDownLatch.await();
  }
}
