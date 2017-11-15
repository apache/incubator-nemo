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
package edu.snu.onyx.runtime.master.scheduler;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.compiler.frontend.beam.transform.DoTransform;
import edu.snu.onyx.compiler.ir.IREdge;
import edu.snu.onyx.compiler.ir.IRVertex;
import edu.snu.onyx.compiler.ir.OperatorVertex;
import edu.snu.onyx.compiler.ir.Transform;
import edu.snu.onyx.common.PubSubEventHandlerWrapper;
import edu.snu.onyx.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.compiler.ir.executionproperty.vertex.ExecutorPlacementProperty;
import edu.snu.onyx.compiler.ir.executionproperty.vertex.ParallelismProperty;
import edu.snu.onyx.compiler.optimizer.Optimizer;
import edu.snu.onyx.compiler.optimizer.examples.EmptyComponents;
import edu.snu.onyx.compiler.optimizer.TestPolicy;
import edu.snu.onyx.runtime.RuntimeTestUtil;
import edu.snu.onyx.runtime.common.comm.ControlMessage;
import edu.snu.onyx.runtime.common.message.MessageSender;
import edu.snu.onyx.runtime.common.metric.MetricMessageHandler;
import edu.snu.onyx.runtime.common.plan.physical.*;
import edu.snu.onyx.runtime.common.state.StageState;
import edu.snu.onyx.runtime.executor.datatransfer.communication.ScatterGather;
import edu.snu.onyx.runtime.master.JobStateManager;
import edu.snu.onyx.runtime.master.PartitionManagerMaster;
import edu.snu.onyx.runtime.master.eventhandler.UpdatePhysicalPlanEventHandler;
import edu.snu.onyx.runtime.master.resource.ContainerManager;
import edu.snu.onyx.runtime.master.resource.ExecutorRepresenter;
import edu.snu.onyx.runtime.master.resource.ResourceSpecification;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.dag.DAGBuilder;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link BatchSingleJobScheduler}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ContainerManager.class, PartitionManagerMaster.class,
    PubSubEventHandlerWrapper.class, UpdatePhysicalPlanEventHandler.class, MetricMessageHandler.class})
public final class BatchSingleJobSchedulerTest {
  private static final Logger LOG = LoggerFactory.getLogger(BatchSingleJobSchedulerTest.class.getName());
  private DAGBuilder<IRVertex, IREdge> irDAGBuilder;
  private Scheduler scheduler;
  private SchedulingPolicy schedulingPolicy;
  private SchedulerRunner schedulerRunner;
  private ContainerManager containerManager;
  private MetricMessageHandler metricMessageHandler;
  private PendingTaskGroupQueue pendingTaskGroupQueue;
  private PubSubEventHandlerWrapper pubSubEventHandler;
  private UpdatePhysicalPlanEventHandler updatePhysicalPlanEventHandler;
  private PartitionManagerMaster partitionManagerMaster = mock(PartitionManagerMaster.class);
  private final MessageSender<ControlMessage.Message> mockMsgSender = mock(MessageSender.class);
  private PhysicalPlanGenerator physicalPlanGenerator;

  private static final int TEST_TIMEOUT_MS = 500;

  // This schedule index will make sure that task group events are not ignored
  private static final int MAGIC_SCHEDULE_ATTEMPT_INDEX = Integer.MAX_VALUE;

  @Before
  public void setUp() throws Exception {
    RuntimeTestUtil.initialize();
    irDAGBuilder = new DAGBuilder<>();
    containerManager = mock(ContainerManager.class);
    metricMessageHandler = mock(MetricMessageHandler.class);
    pendingTaskGroupQueue = new SingleJobTaskGroupQueue();
    schedulingPolicy = new RoundRobinSchedulingPolicy(containerManager, TEST_TIMEOUT_MS);
    schedulerRunner = new SchedulerRunner(schedulingPolicy, pendingTaskGroupQueue);
    pubSubEventHandler = mock(PubSubEventHandlerWrapper.class);
    updatePhysicalPlanEventHandler = mock(UpdatePhysicalPlanEventHandler.class);
    scheduler =
        new BatchSingleJobScheduler(schedulingPolicy, schedulerRunner, pendingTaskGroupQueue,
            partitionManagerMaster, pubSubEventHandler, updatePhysicalPlanEventHandler);

    final Map<String, ExecutorRepresenter> executorRepresenterMap = new HashMap<>();
    when(containerManager.getExecutorRepresenterMap()).thenReturn(executorRepresenterMap);

    final ActiveContext activeContext = mock(ActiveContext.class);
    Mockito.doThrow(new RuntimeException()).when(activeContext).close();

    final ResourceSpecification computeSpec = new ResourceSpecification(ExecutorPlacementProperty.COMPUTE, 1, 0);
    final ExecutorRepresenter a3 = new ExecutorRepresenter("a3", computeSpec, mockMsgSender, activeContext);
    final ExecutorRepresenter a2 = new ExecutorRepresenter("a2", computeSpec, mockMsgSender, activeContext);
    final ExecutorRepresenter a1 = new ExecutorRepresenter("a1", computeSpec, mockMsgSender, activeContext);

    final ResourceSpecification storageSpec = new ResourceSpecification(ExecutorPlacementProperty.TRANSIENT, 1, 0);
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

    physicalPlanGenerator = Tang.Factory.getTang().newInjector().getInstance(PhysicalPlanGenerator.class);
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link BatchSingleJobScheduler}.
   * TaskGroup state changes are explicitly submitted to scheduler instead of executor messages.
   */
  @Test(timeout=10000)
  public void testPull() throws Exception {
    // Build DAG
    final Transform t = new EmptyComponents.EmptyTransform("empty");
    final IRVertex v1 = new OperatorVertex(t);
    v1.setProperty(ParallelismProperty.of(3));
    v1.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v1);

    final IRVertex v2 = new OperatorVertex(t);
    v2.setProperty(ParallelismProperty.of(2));
    v2.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v2);

    final IRVertex v3 = new OperatorVertex(t);
    v3.setProperty(ParallelismProperty.of(3));
    v3.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v3);

    final IRVertex v4 = new OperatorVertex(t);
    v4.setProperty(ParallelismProperty.of(2));
    v4.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v4);

    final IRVertex v5 = new OperatorVertex(new DoTransform(null, null));
    v5.setProperty(ParallelismProperty.of(2));
    v5.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.TRANSIENT));
    irDAGBuilder.addVertex(v5);

    final IREdge e1 = new IREdge(ScatterGather.class, v1, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(ScatterGather.class, v3, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e2);

    final IREdge e4 = new IREdge(ScatterGather.class, v2, v4, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e4);

    final IREdge e5 = new IREdge(ScatterGather.class, v2, v5, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e5);

    final DAG<IRVertex, IREdge> pullIRDAG = Optimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(),
        new TestPolicy(), "");

    scheduleAndCheckJobTermination(pullIRDAG);
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link BatchSingleJobScheduler}.
   * TaskGroup state changes are explicitly submitted to scheduler instead of executor messages.
   */
  @Test(timeout=10000)
  public void testPush() throws Exception {
    // Build DAG
    final Transform t = new EmptyComponents.EmptyTransform("empty");
    final IRVertex v1 = new OperatorVertex(t);
    v1.setProperty(ParallelismProperty.of(3));
    v1.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v1);

    final IRVertex v2 = new OperatorVertex(t);
    v2.setProperty(ParallelismProperty.of(2));
    v2.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v2);

    final IRVertex v3 = new OperatorVertex(t);
    v3.setProperty(ParallelismProperty.of(3));
    v3.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v3);

    final IRVertex v4 = new OperatorVertex(t);
    v4.setProperty(ParallelismProperty.of(2));
    v4.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v4);

    final IRVertex v5 = new OperatorVertex(new DoTransform(null, null));
    v5.setProperty(ParallelismProperty.of(2));
    v5.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.TRANSIENT));
    irDAGBuilder.addVertex(v5);

    final IREdge e1 = new IREdge(ScatterGather.class, v1, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(ScatterGather.class, v3, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e2);

    final IREdge e4 = new IREdge(ScatterGather.class, v2, v4, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e4);

    final IREdge e5 = new IREdge(ScatterGather.class, v2, v5, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e5);

    final DAG<IRVertex, IREdge> pushIRDAG =
        Optimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(),
        new TestPolicy(true), "");

    scheduleAndCheckJobTermination(pushIRDAG);
  }

  private void scheduleAndCheckJobTermination(final DAG<IRVertex, IREdge> irDAG) throws InjectionException {
    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = irDAG.convert(physicalPlanGenerator);

    final PhysicalPlan plan = new PhysicalPlan("TestPlan", physicalDAG, physicalPlanGenerator.getTaskIRVertexMap());
    final JobStateManager jobStateManager = new JobStateManager(plan, partitionManagerMaster, metricMessageHandler, 1);
    scheduler.scheduleJob(plan, jobStateManager);

    // For each ScheduleGroup, test:
    // a) all stages in the ScheduleGroup enters the executing state
    // b) the stages of the next ScheduleGroup are scheduled after the stages of each ScheduleGroup are made "complete".
    for (int i = 0; i < getNumScheduleGroups(irDAG); i++) {
      final int scheduleGroupIdx = i;

      final List<PhysicalStage> scheduleGroupStages = physicalDAG.filterVertices(physicalStage ->
          physicalStage.getScheduleGroupIndex() == scheduleGroupIdx);

      LOG.debug("Checking that all stages of ScheduleGroup {} enter the executing state", scheduleGroupIdx);
      scheduleGroupStages.forEach(physicalStage -> {
        while (jobStateManager.getStageState(physicalStage.getId()).getStateMachine().getCurrentState()
            != StageState.State.EXECUTING) {

        }
      });

      scheduleGroupStages.forEach(physicalStage ->
          RuntimeTestUtil.sendStageCompletionEventToScheduler(
              jobStateManager, scheduler, containerManager, physicalStage, MAGIC_SCHEDULE_ATTEMPT_INDEX));
    }

    LOG.debug("Waiting for job termination after sending stage completion events");
    while (!jobStateManager.checkJobTermination()) {

    }
    assertTrue(jobStateManager.checkJobTermination());
    RuntimeTestUtil.cleanup();
  }

  private int getNumScheduleGroups(final DAG<IRVertex, IREdge> irDAG) {
    final Set<Integer> scheduleGroupSet = new HashSet<>();
    irDAG.getVertices().forEach(irVertex ->
        scheduleGroupSet.add((Integer) irVertex.getProperty(ExecutionProperty.Key.ScheduleGroupIndex)));
    return scheduleGroupSet.size();
  }
}
