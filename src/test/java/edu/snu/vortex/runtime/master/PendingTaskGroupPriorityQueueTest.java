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
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.common.dag.DAGBuilder;
import edu.snu.vortex.compiler.frontend.beam.transform.DoTransform;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.OperatorVertex;
import edu.snu.vortex.compiler.ir.Transform;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.compiler.optimizer.Optimizer;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.runtime.master.scheduler.*;
import org.apache.reef.tang.Tang;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link PendingTaskGroupPriorityQueue}.
 */
public final class PendingTaskGroupPriorityQueueTest {
  private DAGBuilder<IRVertex, IREdge> irDAGBuilder;
  private PendingTaskGroupPriorityQueue pendingTaskGroupPriorityQueue;

  @Before
  public void setUp() {
    irDAGBuilder = new DAGBuilder<>();
    pendingTaskGroupPriorityQueue = new PendingTaskGroupPriorityQueue();
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link PendingTaskGroupPriorityQueue}.
   * Tests whether the dequeued TaskGroups are according to the stage-dependency priority.
   */
  @Test
  public void testSimpleStageDependency() throws Exception {
    final Transform t = mock(Transform.class);
    final IRVertex v1 = new OperatorVertex(t);
    v1.setAttr(Attribute.IntegerKey.Parallelism, 3);
    v1.setAttr(Attribute.Key.Placement, Attribute.Compute);
    irDAGBuilder.addVertex(v1);

    final IRVertex v2 = new OperatorVertex(t);
    v2.setAttr(Attribute.IntegerKey.Parallelism, 2);
    v2.setAttr(Attribute.Key.Placement, Attribute.Compute);
    irDAGBuilder.addVertex(v2);

    final IRVertex v3 = new OperatorVertex(t);
    v3.setAttr(Attribute.IntegerKey.Parallelism, 4);
    v3.setAttr(Attribute.Key.Placement, Attribute.Compute);
    irDAGBuilder.addVertex(v3);

    final IREdge e1 = new IREdge(IREdge.Type.ScatterGather, v1, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(IREdge.Type.ScatterGather, v2, v3, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e2);

    final DAG<IRVertex, IREdge> irDAG = Optimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(),
            Optimizer.PolicyType.TestingPolicy, "");
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

    // Add TaskGroups of the second stage to the queue (this is the earliest point 2nd stage can be scheduled)
    dagOf3Stages.get(1).getTaskGroupList().forEach(taskGroup ->
        pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup(taskGroup, null, null, 0)));

    // The 2nd dequeued TaskGroup should also belong to the 1st stage.
    assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
        dagOf3Stages.get(0).getId());

    // The 3rd dequeued TaskGroup should also belong to the 1st stage.
    ScheduledTaskGroup dequeued = pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get();
    assertEquals(dequeued.getTaskGroup().getStageId(), dagOf3Stages.get(0).getId());

    // The dequeued TaskGroup should belong to the 2nd stage.
    assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
        dagOf3Stages.get(1).getId());

    // Add TaskGroups of the third stage to the queue. (this is the earliest point 3rd stage can be scheduled)
    dagOf3Stages.get(2).getTaskGroupList().forEach(taskGroup ->
        pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup(taskGroup, null, null, 0)));

    // The 2nd dequeued TaskGroup should also belong to the 2nd stage.
    assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
        dagOf3Stages.get(1).getId());

    // The dequeued TaskGroup should belong to the 3rd stage.
    assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
        dagOf3Stages.get(2).getId());
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link PendingTaskGroupPriorityQueue}.
   * Tests whether the dequeued TaskGroups are according to the stage-dependency priority.
   */
  @Test
  public void test2RootStageDependency() throws Exception {
    final Transform t = mock(Transform.class);
    final IRVertex v1 = new OperatorVertex(t);
    v1.setAttr(Attribute.IntegerKey.Parallelism, 3);
    v1.setAttr(Attribute.Key.Placement, Attribute.Compute);
    irDAGBuilder.addVertex(v1);

    final IRVertex v2 = new OperatorVertex(t);
    v2.setAttr(Attribute.IntegerKey.Parallelism, 2);
    v2.setAttr(Attribute.Key.Placement, Attribute.Compute);
    irDAGBuilder.addVertex(v2);

    final IRVertex v3 = new OperatorVertex(new DoTransform(null, null));
    v3.setAttr(Attribute.IntegerKey.Parallelism, 4);
    v3.setAttr(Attribute.Key.Placement, Attribute.Compute);
    irDAGBuilder.addVertex(v3);

    final IREdge e1 = new IREdge(IREdge.Type.ScatterGather, v1, v3, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(IREdge.Type.ScatterGather, v2, v3, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e2);

    final DAG<IRVertex, IREdge> irDAG = Optimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(),
            Optimizer.PolicyType.TestingPolicy, "");
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

    // Add TaskGroups of the third stage to the queue. (this is the earliest point 3rd stage can be scheduled)
    dagOf3Stages.get(2).getTaskGroupList().forEach(taskGroup ->
        pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup(taskGroup, null, null, 0)));

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
    v1.setAttr(Attribute.IntegerKey.Parallelism, 3);
    v1.setAttr(Attribute.Key.Placement, Attribute.Compute);
    irDAGBuilder.addVertex(v1);

    final IRVertex v2 = new OperatorVertex(t);
    v2.setAttr(Attribute.IntegerKey.Parallelism, 2);
    v2.setAttr(Attribute.Key.Placement, Attribute.Compute);
    irDAGBuilder.addVertex(v2);

    final IRVertex v3 = new OperatorVertex(new DoTransform(null, null));
    v3.setAttr(Attribute.IntegerKey.Parallelism, 4);
    v3.setAttr(Attribute.Key.Placement, Attribute.Compute);
    irDAGBuilder.addVertex(v3);

    final IREdge e1 = new IREdge(IREdge.Type.ScatterGather, v1, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(IREdge.Type.ScatterGather, v2, v3, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e2);

    final DAG<IRVertex, IREdge> irDAG = Optimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(),
            Optimizer.PolicyType.TestingPolicy, "");
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

    // Add TaskGroups of the second stage to the queue (this is the earliest point 2nd stage can be scheduled)
    dagOf3Stages.get(1).getTaskGroupList().forEach(taskGroup ->
        pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup(taskGroup, null, null, 0)));

    // The 2nd dequeued TaskGroup should also belong to the 1st stage.
    assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
        dagOf3Stages.get(0).getId());

    // The 3rd dequeued TaskGroup should also belong to the 1st stage.
    ScheduledTaskGroup dequeued = pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get();
    assertEquals(dequeued.getTaskGroup().getStageId(), dagOf3Stages.get(0).getId());

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

    // Add TaskGroups of the third stage to the queue. (this is the earliest point 3rd stage can be scheduled)
    dagOf3Stages.get(2).getTaskGroupList().forEach(taskGroup ->
        pendingTaskGroupPriorityQueue.enqueue(new ScheduledTaskGroup(taskGroup, null, null, 0)));

    // The 2nd dequeued TaskGroup should also belong to the 2nd stage.
    assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
        dagOf3Stages.get(1).getId());

    // The dequeued TaskGroup should belong to the 3rd stage.
    assertEquals(pendingTaskGroupPriorityQueue.dequeueNextTaskGroup().get().getTaskGroup().getStageId(),
        dagOf3Stages.get(2).getId());
  }
}
