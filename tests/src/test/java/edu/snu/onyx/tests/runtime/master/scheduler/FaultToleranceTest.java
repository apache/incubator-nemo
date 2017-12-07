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
import edu.snu.onyx.common.eventhandler.PubSubEventHandlerWrapper;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.ir.vertex.OperatorVertex;
import edu.snu.onyx.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.onyx.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.onyx.compiler.frontend.beam.transform.DoTransform;
import edu.snu.onyx.common.ir.Transform;
import edu.snu.onyx.compiler.optimizer.CompiletimeOptimizer;
import edu.snu.onyx.compiler.optimizer.examples.EmptyComponents;
import edu.snu.onyx.conf.JobConf;
import edu.snu.onyx.tests.runtime.RuntimeTestUtil;
import edu.snu.onyx.runtime.common.comm.ControlMessage;
import edu.snu.onyx.runtime.common.message.MessageSender;
import edu.snu.onyx.runtime.common.plan.physical.*;
import edu.snu.onyx.runtime.common.state.TaskGroupState;
import edu.snu.onyx.runtime.master.JobStateManager;
import edu.snu.onyx.runtime.master.MetricMessageHandler;
import edu.snu.onyx.runtime.master.PartitionManagerMaster;
import edu.snu.onyx.runtime.master.eventhandler.UpdatePhysicalPlanEventHandler;
import edu.snu.onyx.runtime.master.resource.ContainerManager;
import edu.snu.onyx.runtime.master.resource.ExecutorRepresenter;
import edu.snu.onyx.runtime.master.resource.ResourceSpecification;
import edu.snu.onyx.runtime.master.scheduler.*;
import edu.snu.onyx.tests.compiler.optimizer.TestPolicy;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static edu.snu.onyx.runtime.common.state.StageState.State.EXECUTING;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests fault tolerance.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ContainerManager.class, PartitionManagerMaster.class, SchedulerRunner.class,
    PubSubEventHandlerWrapper.class, UpdatePhysicalPlanEventHandler.class, MetricMessageHandler.class})
public final class FaultToleranceTest {
  private static final Logger LOG = LoggerFactory.getLogger(FaultToleranceTest.class.getName());
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
  private static final int MAX_SCHEDULE_ATTEMPT = 3;

  // This schedule index will make sure that task group events are not ignored
  private static final int MAGIC_SCHEDULE_ATTEMPT_INDEX = Integer.MAX_VALUE;

  @Before
  public void setUp() throws Exception {
    RuntimeTestUtil.initialize();
    irDAGBuilder = new DAGBuilder<>();

    metricMessageHandler = mock(MetricMessageHandler.class);
    pubSubEventHandler = mock(PubSubEventHandlerWrapper.class);
    updatePhysicalPlanEventHandler = mock(UpdatePhysicalPlanEventHandler.class);

    final ActiveContext activeContext = mock(ActiveContext.class);
    Mockito.doThrow(new RuntimeException()).when(activeContext).close();

    final ResourceSpecification computeSpec = new ResourceSpecification(ExecutorPlacementProperty.COMPUTE, 2, 0);
    final ExecutorRepresenter a3 = new ExecutorRepresenter("a3", computeSpec, mockMsgSender, activeContext);
    final ExecutorRepresenter a2 = new ExecutorRepresenter("a2", computeSpec, mockMsgSender, activeContext);
    final ExecutorRepresenter a1 = new ExecutorRepresenter("a1", computeSpec, mockMsgSender, activeContext);

    final Map<String, ExecutorRepresenter> executorRepresenterMap = new HashMap<>();
    executorRepresenterMap.put(a1.getExecutorId(), a1);
    executorRepresenterMap.put(a2.getExecutorId(), a2);
    executorRepresenterMap.put(a3.getExecutorId(), a3);

    containerManager = mock(ContainerManager.class);
    when(containerManager.getExecutorRepresenterMap()).thenReturn(executorRepresenterMap);
    when(containerManager.getFailedExecutorRepresenterMap()).thenReturn(executorRepresenterMap);

    pendingTaskGroupQueue = new SingleJobTaskGroupQueue();
    schedulingPolicy = new RoundRobinSchedulingPolicy(containerManager, TEST_TIMEOUT_MS);
    schedulerRunner = mock(SchedulerRunner.class);//new SchedulerRunner(schedulingPolicy, pendingTaskGroupQueue);
    scheduler =
        new BatchSingleJobScheduler(schedulingPolicy, schedulerRunner, pendingTaskGroupQueue,
            partitionManagerMaster, pubSubEventHandler, updatePhysicalPlanEventHandler);

    // Add nodes
    scheduler.onExecutorAdded(a1.getExecutorId());
    scheduler.onExecutorAdded(a2.getExecutorId());
    scheduler.onExecutorAdded(a3.getExecutorId());

    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(JobConf.DAGDirectory.class, "");
    physicalPlanGenerator = injector.getInstance(PhysicalPlanGenerator.class);
  }

  /**
   * Tests fault tolerance after a container removal.
   */
  @Test(timeout=10000)
  public void testContainerRemoval() throws Exception {
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
    v5.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v5);

    final IREdge e1 = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, v1, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, v3, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e2);

    final IREdge e3 = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, v2, v4, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e3);

    final IREdge e4 = new IREdge(DataCommunicationPatternProperty.Value.OneToOne, v4, v5, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e4);

    final DAG<IRVertex, IREdge> irDAG = CompiletimeOptimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(),
        new TestPolicy(), "");
    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = irDAG.convert(physicalPlanGenerator);

    final PhysicalPlan plan = new PhysicalPlan("TestPlan", physicalDAG, physicalPlanGenerator.getTaskIRVertexMap());
    final JobStateManager jobStateManager =
        new JobStateManager(plan, partitionManagerMaster, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);
    scheduler.scheduleJob(plan, jobStateManager);

    final List<PhysicalStage> dagOf4Stages = plan.getStageDAG().getTopologicalSort();

    for (final PhysicalStage stage : dagOf4Stages) {
      if (stage.getScheduleGroupIndex() == 0) {

        // There are 3 executors, each of capacity 2, and there are 6 TaskGroups in ScheduleGroup 0.
        RuntimeTestUtil.mockSchedulerRunner(pendingTaskGroupQueue, schedulingPolicy, false);
        assertTrue(pendingTaskGroupQueue.isEmpty());
        stage.getTaskGroupList().forEach(taskGroup -> {
          jobStateManager.onTaskGroupStateChanged(taskGroup, TaskGroupState.State.EXECUTING);
          RuntimeTestUtil.sendTaskGroupStateEventToScheduler(scheduler, containerManager,
              taskGroup.getTaskGroupId(), TaskGroupState.State.COMPLETE, 1, null);
            });
      } else if (stage.getScheduleGroupIndex() == 1) {
        // There are 3 executors, each of capacity 2, and there are 2 TaskGroups in ScheduleGroup 1.
        RuntimeTestUtil.mockSchedulerRunner(pendingTaskGroupQueue, schedulingPolicy, false);
        stage.getTaskGroupList().forEach(taskGroup ->
            jobStateManager.onTaskGroupStateChanged(taskGroup, TaskGroupState.State.EXECUTING));

        // Due to round robin scheduling, "a2" is assured to have a running TaskGroup.
        scheduler.onExecutorRemoved("a2");

        while (jobStateManager.getStageState(stage.getId()).getStateMachine().getCurrentState() != EXECUTING) {

        }
        assertEquals(jobStateManager.getAttemptCountForStage(stage.getId()), 2);

        RuntimeTestUtil.mockSchedulerRunner(pendingTaskGroupQueue, schedulingPolicy, false);
        assertTrue(pendingTaskGroupQueue.isEmpty());
        stage.getTaskGroupList().forEach(taskGroup -> {
          final Enum taskGroupState =
              jobStateManager.getTaskGroupState(taskGroup.getTaskGroupId()).getStateMachine().getCurrentState();
          if (taskGroupState == TaskGroupState.State.READY) {
            jobStateManager.onTaskGroupStateChanged(taskGroup, TaskGroupState.State.EXECUTING);
          }
          RuntimeTestUtil.sendTaskGroupStateEventToScheduler(scheduler, containerManager,
              taskGroup.getTaskGroupId(), TaskGroupState.State.COMPLETE, 1, null);
        });
      } else {
        // There are 2 executors, each of capacity 2, and there are 2 TaskGroups in ScheduleGroup 2.
        // Schedule only the first TaskGroup
        RuntimeTestUtil.mockSchedulerRunner(pendingTaskGroupQueue, schedulingPolicy, true);

        boolean isFirstTaskGroup = true;
        for (final TaskGroup taskGroup : stage.getTaskGroupList()) {
          // When a TaskGroup fails while the siblings are still in the queue,
          if (isFirstTaskGroup) {
            jobStateManager.onTaskGroupStateChanged(taskGroup, TaskGroupState.State.EXECUTING);

            // Due to round robin scheduling, "a3" is assured to have a running TaskGroup.
            scheduler.onExecutorRemoved("a3");
            isFirstTaskGroup = false;
          } else {
            // Test that the sibling TaskGroup state remains unchanged.
            assertEquals(
                jobStateManager.getTaskGroupState(taskGroup.getTaskGroupId()).getStateMachine().getCurrentState(),
                TaskGroupState.State.READY);
          }
        }
      }
    }

    RuntimeTestUtil.cleanup();
  }

  /**
   * Tests fault tolerance after an output write failure.
   */
  @Test(timeout=10000)
  public void testOutputFailure() throws Exception {
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
    v5.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v5);

    final IREdge e1 = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, v1, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, v3, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e2);

    final IREdge e3 = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, v2, v4, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e3);

    final IREdge e4 = new IREdge(DataCommunicationPatternProperty.Value.OneToOne, v4, v5, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e4);

    final DAG<IRVertex, IREdge> irDAG = CompiletimeOptimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(),
        new TestPolicy(), "");
    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = irDAG.convert(physicalPlanGenerator);

    final PhysicalPlan plan = new PhysicalPlan("TestPlan", physicalDAG, physicalPlanGenerator.getTaskIRVertexMap());
    final JobStateManager jobStateManager =
        new JobStateManager(plan, partitionManagerMaster, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);
    scheduler.scheduleJob(plan, jobStateManager);

    final List<PhysicalStage> dagOf4Stages = plan.getStageDAG().getTopologicalSort();

    for (final PhysicalStage stage : dagOf4Stages) {
      if (stage.getScheduleGroupIndex() == 0) {

        // There are 3 executors, each of capacity 2, and there are 6 TaskGroups in ScheduleGroup 0.
        RuntimeTestUtil.mockSchedulerRunner(pendingTaskGroupQueue, schedulingPolicy, false);
        assertTrue(pendingTaskGroupQueue.isEmpty());
        stage.getTaskGroupList().forEach(taskGroup -> {
          jobStateManager.onTaskGroupStateChanged(taskGroup, TaskGroupState.State.EXECUTING);
          RuntimeTestUtil.sendTaskGroupStateEventToScheduler(scheduler, containerManager,
              taskGroup.getTaskGroupId(), TaskGroupState.State.COMPLETE, 1, null);
        });
      } else if (stage.getScheduleGroupIndex() == 1) {
        // There are 3 executors, each of capacity 2, and there are 2 TaskGroups in ScheduleGroup 1.
        RuntimeTestUtil.mockSchedulerRunner(pendingTaskGroupQueue, schedulingPolicy, false);
        assertTrue(pendingTaskGroupQueue.isEmpty());
        stage.getTaskGroupList().forEach(taskGroup -> {
          jobStateManager.onTaskGroupStateChanged(taskGroup, TaskGroupState.State.EXECUTING);
          RuntimeTestUtil.sendTaskGroupStateEventToScheduler(scheduler, containerManager,
              taskGroup.getTaskGroupId(), TaskGroupState.State.FAILED_RECOVERABLE, 1,
              TaskGroupState.RecoverableFailureCause.OUTPUT_WRITE_FAILURE);
        });

        while (jobStateManager.getStageState(stage.getId()).getStateMachine().getCurrentState() != EXECUTING) {

        }

        assertEquals(jobStateManager.getAttemptCountForStage(stage.getId()), 3);
        assertFalse(pendingTaskGroupQueue.isEmpty());
        stage.getTaskGroupList().forEach(taskGroup ->
            assertEquals(jobStateManager.getTaskGroupState(taskGroup.getTaskGroupId()).getStateMachine().getCurrentState(),
                TaskGroupState.State.READY));
      }
    }

    RuntimeTestUtil.cleanup();
  }

  /**
   * Tests fault tolerance after an input read failure.
   */
  @Test(timeout=10000)
  public void testInputReadFailure() throws Exception {
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
    v5.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));
    irDAGBuilder.addVertex(v5);

    final IREdge e1 = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, v1, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, v3, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e2);

    final IREdge e4 = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, v2, v4, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e4);

    final IREdge e5 = new IREdge(DataCommunicationPatternProperty.Value.OneToOne, v4, v5, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e5);

    final DAG<IRVertex, IREdge> irDAG = CompiletimeOptimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(),
        new TestPolicy(), "");
    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = irDAG.convert(physicalPlanGenerator);

    final PhysicalPlan plan = new PhysicalPlan("TestPlan", physicalDAG, physicalPlanGenerator.getTaskIRVertexMap());
    final JobStateManager jobStateManager =
        new JobStateManager(plan, partitionManagerMaster, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);
    scheduler.scheduleJob(plan, jobStateManager);

    final List<PhysicalStage> dagOf4Stages = plan.getStageDAG().getTopologicalSort();

    for (final PhysicalStage stage : dagOf4Stages) {
      if (stage.getScheduleGroupIndex() == 0) {

        // There are 3 executors, each of capacity 2, and there are 6 TaskGroups in ScheduleGroup 0.
        RuntimeTestUtil.mockSchedulerRunner(pendingTaskGroupQueue, schedulingPolicy, false);
        assertTrue(pendingTaskGroupQueue.isEmpty());
        stage.getTaskGroupList().forEach(taskGroup -> {
          jobStateManager.onTaskGroupStateChanged(taskGroup, TaskGroupState.State.EXECUTING);
          RuntimeTestUtil.sendTaskGroupStateEventToScheduler(scheduler, containerManager,
              taskGroup.getTaskGroupId(), TaskGroupState.State.COMPLETE, 1, null);
        });
      } else if (stage.getScheduleGroupIndex() == 1) {
        // There are 3 executors, each of capacity 2, and there are 2 TaskGroups in ScheduleGroup 1.
        RuntimeTestUtil.mockSchedulerRunner(pendingTaskGroupQueue, schedulingPolicy, false);
        stage.getTaskGroupList().forEach(taskGroup ->
            jobStateManager.onTaskGroupStateChanged(taskGroup, TaskGroupState.State.EXECUTING));

        stage.getTaskGroupList().forEach(taskGroup ->
          RuntimeTestUtil.sendTaskGroupStateEventToScheduler(scheduler, containerManager,
              taskGroup.getTaskGroupId(), TaskGroupState.State.FAILED_RECOVERABLE, 1,
              TaskGroupState.RecoverableFailureCause.INPUT_READ_FAILURE));

        while (jobStateManager.getStageState(stage.getId()).getStateMachine().getCurrentState() != EXECUTING) {

        }

        assertEquals(jobStateManager.getAttemptCountForStage(stage.getId()), 2);
        stage.getTaskGroupList().forEach(taskGroup ->
        assertEquals(jobStateManager.getTaskGroupState(taskGroup.getTaskGroupId()).getStateMachine().getCurrentState(),
            TaskGroupState.State.READY));
      }
    }

    RuntimeTestUtil.cleanup();
  }
}
