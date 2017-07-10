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
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.OperatorVertex;
import edu.snu.vortex.compiler.ir.Transform;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.TestUtil;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageSender;
import edu.snu.vortex.runtime.common.plan.logical.LogicalDAGGenerator;
import edu.snu.vortex.runtime.common.plan.logical.Stage;
import edu.snu.vortex.runtime.common.plan.logical.StageEdge;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.runtime.common.state.JobState;
import edu.snu.vortex.runtime.common.state.StageState;
import edu.snu.vortex.runtime.master.resource.ContainerManager;
import edu.snu.vortex.runtime.master.resource.ExecutorRepresenter;
import edu.snu.vortex.runtime.master.resource.ResourceSpecification;
import edu.snu.vortex.runtime.master.scheduler.*;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.common.dag.DAGBuilder;
import org.apache.reef.driver.context.ActiveContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link BatchScheduler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(ContainerManager.class)
public final class BatchSchedulerTest {
  private DAGBuilder<IRVertex, IREdge> irDAGBuilder;
  private Scheduler scheduler;
  private SchedulingPolicy schedulingPolicy;
  private ContainerManager containerManager;
  private PendingTaskGroupQueue pendingTaskGroupQueue;
  private PartitionManagerMaster partitionManagerMaster;
  private final MessageSender<ControlMessage.Message> mockMsgSender = mock(MessageSender.class);

  private static final int TEST_TIMEOUT_MS = 1000;

  // This schedule index will make sure that task group events are not ignored
  private static final int MAGIC_SCHEDULE_ATTEMPT_INDEX = Integer.MAX_VALUE;

  @Before
  public void setUp() {
    irDAGBuilder = new DAGBuilder<>();
    containerManager = mock(ContainerManager.class);
    pendingTaskGroupQueue = new PendingTaskGroupQueue();
    schedulingPolicy = new RoundRobinSchedulingPolicy(containerManager, TEST_TIMEOUT_MS);
    partitionManagerMaster = new PartitionManagerMaster();
    scheduler = new BatchScheduler(partitionManagerMaster, schedulingPolicy, pendingTaskGroupQueue);
    partitionManagerMaster = new PartitionManagerMaster();

    final Map<String, ExecutorRepresenter> executorRepresenterMap = new HashMap<>();
    when(containerManager.getExecutorRepresenterMap()).thenReturn(executorRepresenterMap);

    schedulingPolicy = new RoundRobinSchedulingPolicy(containerManager, TEST_TIMEOUT_MS);

    final ActiveContext activeContext = mock(ActiveContext.class);
    Mockito.doThrow(new RuntimeException()).when(activeContext).close();

    final ResourceSpecification computeSpec = new ResourceSpecification(Attribute.Compute, 1, 0);
    final ExecutorRepresenter a3 = new ExecutorRepresenter("a3", computeSpec, mockMsgSender, activeContext);
    final ExecutorRepresenter a2 = new ExecutorRepresenter("a2", computeSpec, mockMsgSender, activeContext);
    final ExecutorRepresenter a1 = new ExecutorRepresenter("a1", computeSpec, mockMsgSender, activeContext);

    final ResourceSpecification storageSpec = new ResourceSpecification(Attribute.Storage, 1, 0);
    final ExecutorRepresenter b2 = new ExecutorRepresenter("b2", storageSpec, mockMsgSender, activeContext);
    final ExecutorRepresenter b1 = new ExecutorRepresenter("b1", storageSpec, mockMsgSender, activeContext);

    executorRepresenterMap.put(a1.getExecutorId(), a1);
    executorRepresenterMap.put(a2.getExecutorId(), a2);
    executorRepresenterMap.put(a3.getExecutorId(), a3);
    executorRepresenterMap.put(b1.getExecutorId(), b1);
    executorRepresenterMap.put(b2.getExecutorId(), b2);

    // Add compute nodes
    scheduler.onExecutorAdded(a1.getExecutorId());
    scheduler.onExecutorAdded(a2.getExecutorId());
    scheduler.onExecutorAdded(a3.getExecutorId());

    // Add storage nodes
    scheduler.onExecutorAdded(b1.getExecutorId());
    scheduler.onExecutorAdded(b2.getExecutorId());
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link BatchScheduler}.
   * TaskGroup state changes are explicitly submitted to scheduler instead of executor messages.
   */
  @Test
  public void testMultiInputOutputScheduling() {

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
    v3.setAttr(Attribute.IntegerKey.Parallelism, 3);
    v3.setAttr(Attribute.Key.Placement, Attribute.Compute);
    irDAGBuilder.addVertex(v3);

    final IRVertex v4 = new OperatorVertex(t);
    v4.setAttr(Attribute.IntegerKey.Parallelism, 2);
    v4.setAttr(Attribute.Key.Placement, Attribute.Storage);
    irDAGBuilder.addVertex(v4);

    final IRVertex v5 = new OperatorVertex(t);
    v5.setAttr(Attribute.IntegerKey.Parallelism, 2);
    v5.setAttr(Attribute.Key.Placement, Attribute.Storage);
    irDAGBuilder.addVertex(v5);

    final IREdge e1 = new IREdge(IREdge.Type.ScatterGather, v1, v2, Coder.DUMMY_CODER);
    e1.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
    e1.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);
    irDAGBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(IREdge.Type.ScatterGather, v3, v2, Coder.DUMMY_CODER);
    e2.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
    e2.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);
    irDAGBuilder.connectVertices(e2);

    final IREdge e4 = new IREdge(IREdge.Type.ScatterGather, v2, v4, Coder.DUMMY_CODER);
    e4.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
    e4.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);
    irDAGBuilder.connectVertices(e4);

    final IREdge e5 = new IREdge(IREdge.Type.ScatterGather, v2, v5, Coder.DUMMY_CODER);
    e5.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
    e5.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);
    irDAGBuilder.connectVertices(e5);

    final DAG<IRVertex, IREdge> irDAG = irDAGBuilder.buildWithoutSourceSinkCheck();
    final DAG<Stage, StageEdge> logicalDAG = irDAG.convert(new LogicalDAGGenerator());
    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = logicalDAG.convert(new PhysicalDAGGenerator());

    final JobStateManager jobStateManager =
        scheduler.scheduleJob(new PhysicalPlan("TestPlan", physicalDAG), 1);

    // Start off with the root stages.
    physicalDAG.getRootVertices().forEach(physicalStage ->
        TestUtil.sendStageCompletionEventToScheduler(
            jobStateManager, scheduler, containerManager, physicalStage, MAGIC_SCHEDULE_ATTEMPT_INDEX));

    // Then, for the rest of the stages.
    while (!jobStateManager.checkJobTermination()) {
      final List<PhysicalStage> stageList = physicalDAG.getTopologicalSort();
      stageList.forEach(physicalStage ->
          TestUtil.sendStageCompletionEventToScheduler(
              jobStateManager, scheduler, containerManager, physicalStage, MAGIC_SCHEDULE_ATTEMPT_INDEX));
    }

    // Check that the job have completed (not failed)
    assertTrue(jobStateManager.getJobState().getStateMachine().getCurrentState() == JobState.State.COMPLETE);

    // Check that all stages have completed.
    physicalDAG.getVertices().forEach(physicalStage ->
        assertTrue(jobStateManager.getStageState(physicalStage.getId()).getStateMachine().getCurrentState()
            == StageState.State.COMPLETE));
  }
}
