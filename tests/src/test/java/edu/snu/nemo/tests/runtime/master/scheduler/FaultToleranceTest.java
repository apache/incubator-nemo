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
package edu.snu.nemo.tests.runtime.master.scheduler;

import edu.snu.nemo.common.coder.Coder;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import edu.snu.nemo.compiler.optimizer.CompiletimeOptimizer;
import edu.snu.nemo.compiler.optimizer.examples.EmptyComponents;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.runtime.master.resource.ExecutorRegistry;
import edu.snu.nemo.tests.runtime.RuntimeTestUtil;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.message.MessageSender;
import edu.snu.nemo.runtime.common.plan.physical.*;
import edu.snu.nemo.runtime.common.state.TaskGroupState;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.master.MetricMessageHandler;
import edu.snu.nemo.runtime.master.BlockManagerMaster;
import edu.snu.nemo.runtime.master.eventhandler.UpdatePhysicalPlanEventHandler;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import edu.snu.nemo.runtime.master.resource.ResourceSpecification;
import edu.snu.nemo.runtime.master.scheduler.*;
import edu.snu.nemo.tests.compiler.optimizer.policy.TestPolicy;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.tang.Injector;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static edu.snu.nemo.runtime.common.state.StageState.State.COMPLETE;
import static edu.snu.nemo.runtime.common.state.StageState.State.EXECUTING;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests fault tolerance.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockManagerMaster.class, SchedulerRunner.class,
    PubSubEventHandlerWrapper.class, UpdatePhysicalPlanEventHandler.class, MetricMessageHandler.class})
public final class FaultToleranceTest {
  private static final Logger LOG = LoggerFactory.getLogger(FaultToleranceTest.class.getName());
  private DAGBuilder<IRVertex, IREdge> irDAGBuilder;
  private Scheduler scheduler;
  private SchedulingPolicy schedulingPolicy;
  private SchedulerRunner schedulerRunner;
  private ExecutorRegistry executorRegistry;

  private MetricMessageHandler metricMessageHandler;
  private PendingTaskGroupQueue pendingTaskGroupQueue;
  private PubSubEventHandlerWrapper pubSubEventHandler;
  private UpdatePhysicalPlanEventHandler updatePhysicalPlanEventHandler;
  private BlockManagerMaster blockManagerMaster = mock(BlockManagerMaster.class);
  private final MessageSender<ControlMessage.Message> mockMsgSender = mock(MessageSender.class);
  private final ExecutorService serExecutorService = Executors.newSingleThreadExecutor();
  private PhysicalPlanGenerator physicalPlanGenerator;

  private static final int TEST_TIMEOUT_MS = 500;
  private static final int MAX_SCHEDULE_ATTEMPT = 3;

  @Before
  public void setUp() throws Exception {
    RuntimeTestUtil.initialize();
    irDAGBuilder = new DAGBuilder<>();

    metricMessageHandler = mock(MetricMessageHandler.class);
    pubSubEventHandler = mock(PubSubEventHandlerWrapper.class);
    updatePhysicalPlanEventHandler = mock(UpdatePhysicalPlanEventHandler.class);

    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(JobConf.DAGDirectory.class, "");
    physicalPlanGenerator = injector.getInstance(PhysicalPlanGenerator.class);
  }

  private void setUpExecutors(final Collection<ExecutorRepresenter> executors,
                              final boolean useMockSchedulerRunner) throws InjectionException {
    executorRegistry = Tang.Factory.getTang().newInjector().getInstance(ExecutorRegistry.class);

    pendingTaskGroupQueue = new SingleJobTaskGroupQueue();
    schedulingPolicy = new RoundRobinSchedulingPolicy(executorRegistry, TEST_TIMEOUT_MS);

    if (useMockSchedulerRunner) {
      schedulerRunner = mock(SchedulerRunner.class);
    } else {
      schedulerRunner = new SchedulerRunner(schedulingPolicy, pendingTaskGroupQueue);
    }
    scheduler =
        new BatchSingleJobScheduler(schedulingPolicy, schedulerRunner, pendingTaskGroupQueue,
            blockManagerMaster, pubSubEventHandler, updatePhysicalPlanEventHandler);

    // Add nodes
    for (final ExecutorRepresenter executor : executors) {
      executorRegistry.registerRepresenter(executor);
      scheduler.onExecutorAdded(executor.getExecutorId());
    }
  }

  private PhysicalPlan buildPlan() throws Exception {
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

    final IRVertex v5 = new OperatorVertex(t);
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

    final DAG<IRVertex, IREdge> irDAG =
        CompiletimeOptimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(), new TestPolicy(), "");
    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = irDAG.convert(physicalPlanGenerator);

    return new PhysicalPlan("TestPlan", physicalDAG, physicalPlanGenerator.getTaskIRVertexMap());
  }

  /**
   * Tests fault tolerance after a container removal.
   */
  @Test(timeout=10000)
  public void testContainerRemoval() throws Exception {
    final ActiveContext activeContext = mock(ActiveContext.class);
    Mockito.doThrow(new RuntimeException()).when(activeContext).close();

    final ResourceSpecification computeSpec = new ResourceSpecification(ExecutorPlacementProperty.COMPUTE, 2, 0);
    final Function<String, ExecutorRepresenter> executorRepresenterGenerator = executorId ->
        new ExecutorRepresenter(executorId, computeSpec, mockMsgSender, activeContext, serExecutorService, executorId);
    final ExecutorRepresenter a3 = executorRepresenterGenerator.apply("a3");
    final ExecutorRepresenter a2 = executorRepresenterGenerator.apply("a2");
    final ExecutorRepresenter a1 = executorRepresenterGenerator.apply("a1");

    final List<ExecutorRepresenter> executors = new ArrayList<>();
    executors.add(a1);
    executors.add(a2);
    executors.add(a3);

    setUpExecutors(executors, true);
    final PhysicalPlan plan = buildPlan();
    final JobStateManager jobStateManager =
        new JobStateManager(plan, blockManagerMaster, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);
    scheduler.scheduleJob(plan, jobStateManager);

    final List<PhysicalStage> dagOf4Stages = plan.getStageDAG().getTopologicalSort();

    for (final PhysicalStage stage : dagOf4Stages) {
      if (stage.getScheduleGroupIndex() == 0) {

        // There are 3 executors, each of capacity 2, and there are 6 TaskGroups in ScheduleGroup 0.
        RuntimeTestUtil.mockSchedulerRunner(pendingTaskGroupQueue, schedulingPolicy, jobStateManager, false);
        assertTrue(pendingTaskGroupQueue.isEmpty());
        stage.getTaskGroupIds().forEach(taskGroupId ->
          RuntimeTestUtil.sendTaskGroupStateEventToScheduler(scheduler, executorRegistry,
              taskGroupId, TaskGroupState.State.COMPLETE, 1));
      } else if (stage.getScheduleGroupIndex() == 1) {
        // There are 3 executors, each of capacity 2, and there are 2 TaskGroups in ScheduleGroup 1.
        RuntimeTestUtil.mockSchedulerRunner(pendingTaskGroupQueue, schedulingPolicy, jobStateManager, false);

        // Due to round robin scheduling, "a2" is assured to have a running TaskGroup.
        executorRegistry.setRepresenterAsFailed("a2");
        scheduler.onExecutorRemoved("a2");

        while (jobStateManager.getStageState(stage.getId()).getStateMachine().getCurrentState() != EXECUTING) {

        }
        assertEquals(jobStateManager.getAttemptCountForStage(stage.getId()), 2);

        RuntimeTestUtil.mockSchedulerRunner(pendingTaskGroupQueue, schedulingPolicy, jobStateManager, false);
        assertTrue(pendingTaskGroupQueue.isEmpty());
        stage.getTaskGroupIds().forEach(taskGroupId ->
          RuntimeTestUtil.sendTaskGroupStateEventToScheduler(scheduler, executorRegistry,
              taskGroupId, TaskGroupState.State.COMPLETE, 1));
      } else {
        // There are 2 executors, each of capacity 2, and there are 2 TaskGroups in ScheduleGroup 2.
        // Schedule only the first TaskGroup
        RuntimeTestUtil.mockSchedulerRunner(pendingTaskGroupQueue, schedulingPolicy, jobStateManager, true);

        boolean first = true;
        for (final String taskGroupId : stage.getTaskGroupIds()) {
          // When a TaskGroup fails while the siblings are still in the queue,
          if (first) {
            // Due to round robin scheduling, "a3" is assured to have a running TaskGroup.
            executorRegistry.setRepresenterAsFailed("a3");
            scheduler.onExecutorRemoved("a3");
            first = false;
          } else {
            // Test that the sibling TaskGroup state remains unchanged.
            assertEquals(
                jobStateManager.getTaskGroupState(taskGroupId).getStateMachine().getCurrentState(),
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
    final ActiveContext activeContext = mock(ActiveContext.class);
    Mockito.doThrow(new RuntimeException()).when(activeContext).close();

    final ResourceSpecification computeSpec = new ResourceSpecification(ExecutorPlacementProperty.COMPUTE, 2, 0);
    final Function<String, ExecutorRepresenter> executorRepresenterGenerator = executorId ->
        new ExecutorRepresenter(executorId, computeSpec, mockMsgSender, activeContext, serExecutorService, executorId);
    final ExecutorRepresenter a3 = executorRepresenterGenerator.apply("a3");
    final ExecutorRepresenter a2 = executorRepresenterGenerator.apply("a2");
    final ExecutorRepresenter a1 = executorRepresenterGenerator.apply("a1");

    final List<ExecutorRepresenter> executors = new ArrayList<>();
    executors.add(a1);
    executors.add(a2);
    executors.add(a3);
    setUpExecutors(executors, true);

    final PhysicalPlan plan = buildPlan();
    final JobStateManager jobStateManager =
        new JobStateManager(plan, blockManagerMaster, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);
    scheduler.scheduleJob(plan, jobStateManager);

    final List<PhysicalStage> dagOf4Stages = plan.getStageDAG().getTopologicalSort();

    for (final PhysicalStage stage : dagOf4Stages) {
      if (stage.getScheduleGroupIndex() == 0) {

        // There are 3 executors, each of capacity 2, and there are 6 TaskGroups in ScheduleGroup 0.
        RuntimeTestUtil.mockSchedulerRunner(pendingTaskGroupQueue, schedulingPolicy, jobStateManager, false);
        assertTrue(pendingTaskGroupQueue.isEmpty());
        stage.getTaskGroupIds().forEach(taskGroupId ->
          RuntimeTestUtil.sendTaskGroupStateEventToScheduler(scheduler, executorRegistry,
              taskGroupId, TaskGroupState.State.COMPLETE, 1));
      } else if (stage.getScheduleGroupIndex() == 1) {
        // There are 3 executors, each of capacity 2, and there are 2 TaskGroups in ScheduleGroup 1.
        RuntimeTestUtil.mockSchedulerRunner(pendingTaskGroupQueue, schedulingPolicy, jobStateManager, false);
        assertTrue(pendingTaskGroupQueue.isEmpty());
        stage.getTaskGroupIds().forEach(taskGroupId ->
          RuntimeTestUtil.sendTaskGroupStateEventToScheduler(scheduler, executorRegistry,
              taskGroupId, TaskGroupState.State.FAILED_RECOVERABLE, 1,
              TaskGroupState.RecoverableFailureCause.OUTPUT_WRITE_FAILURE));

        while (jobStateManager.getStageState(stage.getId()).getStateMachine().getCurrentState() != EXECUTING) {

        }

        assertEquals(jobStateManager.getAttemptCountForStage(stage.getId()), 3);
        assertFalse(pendingTaskGroupQueue.isEmpty());
        stage.getTaskGroupIds().forEach(taskGroupId -> {
          assertEquals(jobStateManager.getTaskGroupState(taskGroupId).getStateMachine().getCurrentState(),
              TaskGroupState.State.READY);
        });
      }
    }

    RuntimeTestUtil.cleanup();
  }

  /**
   * Tests fault tolerance after an input read failure.
   */
  @Test(timeout=10000)
  public void testInputReadFailure() throws Exception {
    final ActiveContext activeContext = mock(ActiveContext.class);
    Mockito.doThrow(new RuntimeException()).when(activeContext).close();

    final ResourceSpecification computeSpec = new ResourceSpecification(ExecutorPlacementProperty.COMPUTE, 2, 0);
    final Function<String, ExecutorRepresenter> executorRepresenterGenerator = executorId ->
        new ExecutorRepresenter(executorId, computeSpec, mockMsgSender, activeContext, serExecutorService, executorId);
    final ExecutorRepresenter a3 = executorRepresenterGenerator.apply("a3");
    final ExecutorRepresenter a2 = executorRepresenterGenerator.apply("a2");
    final ExecutorRepresenter a1 = executorRepresenterGenerator.apply("a1");

    final List<ExecutorRepresenter> executors = new ArrayList<>();
    executors.add(a1);
    executors.add(a2);
    executors.add(a3);
    setUpExecutors(executors, true);

    final PhysicalPlan plan = buildPlan();
    final JobStateManager jobStateManager =
        new JobStateManager(plan, blockManagerMaster, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);
    scheduler.scheduleJob(plan, jobStateManager);

    final List<PhysicalStage> dagOf4Stages = plan.getStageDAG().getTopologicalSort();

    for (final PhysicalStage stage : dagOf4Stages) {
      if (stage.getScheduleGroupIndex() == 0) {

        // There are 3 executors, each of capacity 2, and there are 6 TaskGroups in ScheduleGroup 0.
        RuntimeTestUtil.mockSchedulerRunner(pendingTaskGroupQueue, schedulingPolicy, jobStateManager, false);
        assertTrue(pendingTaskGroupQueue.isEmpty());
        stage.getTaskGroupIds().forEach(taskGroupId ->
          RuntimeTestUtil.sendTaskGroupStateEventToScheduler(scheduler, executorRegistry,
              taskGroupId, TaskGroupState.State.COMPLETE, 1));
      } else if (stage.getScheduleGroupIndex() == 1) {
        // There are 3 executors, each of capacity 2, and there are 2 TaskGroups in ScheduleGroup 1.
        RuntimeTestUtil.mockSchedulerRunner(pendingTaskGroupQueue, schedulingPolicy, jobStateManager, false);

        stage.getTaskGroupIds().forEach(taskGroupId ->
          RuntimeTestUtil.sendTaskGroupStateEventToScheduler(scheduler, executorRegistry,
              taskGroupId, TaskGroupState.State.FAILED_RECOVERABLE, 1,
              TaskGroupState.RecoverableFailureCause.INPUT_READ_FAILURE));

        while (jobStateManager.getStageState(stage.getId()).getStateMachine().getCurrentState() != EXECUTING) {

        }

        assertEquals(jobStateManager.getAttemptCountForStage(stage.getId()), 2);
        stage.getTaskGroupIds().forEach(taskGroupId -> {
          assertEquals(jobStateManager.getTaskGroupState(taskGroupId).getStateMachine().getCurrentState(),
              TaskGroupState.State.READY);
        });
      }
    }

    RuntimeTestUtil.cleanup();
  }

  /**
   * Tests the rescheduling of TaskGroups upon a failure.
   */
  @Test(timeout=10000)
  public void testTaskGroupReexecutionForFailure() throws Exception {
  final ActiveContext activeContext = mock(ActiveContext.class);
    Mockito.doThrow(new RuntimeException()).when(activeContext).close();

    final ResourceSpecification computeSpec = new ResourceSpecification(ExecutorPlacementProperty.COMPUTE, 2, 0);
    final Function<String, ExecutorRepresenter> executorRepresenterGenerator = executorId ->
        new ExecutorRepresenter(executorId, computeSpec, mockMsgSender, activeContext, serExecutorService, executorId);
    final ExecutorRepresenter a3 = executorRepresenterGenerator.apply("a3");
    final ExecutorRepresenter a2 = executorRepresenterGenerator.apply("a2");
    final ExecutorRepresenter a1 = executorRepresenterGenerator.apply("a1");

    final List<ExecutorRepresenter> executors = new ArrayList<>();
    executors.add(a1);
    executors.add(a2);
    executors.add(a3);

    setUpExecutors(executors, false);

    final PhysicalPlan plan = buildPlan();
    final JobStateManager jobStateManager =
        new JobStateManager(plan, blockManagerMaster, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);

    scheduler.scheduleJob(plan, jobStateManager);
    executorRegistry.setRepresenterAsFailed("a2");
    scheduler.onExecutorRemoved("a2");

    final List<PhysicalStage> dagOf4Stages = plan.getStageDAG().getTopologicalSort();

    for (final PhysicalStage stage : dagOf4Stages) {
      while (jobStateManager.getStageState(stage.getId()).getStateMachine().getCurrentState() != COMPLETE) {
        final Set<String> a1RunningTaskGroups = new HashSet<>(a1.getRunningTaskGroups());
        final Set<String> a3RunningTaskGroups = new HashSet<>(a3.getRunningTaskGroups());

        a1RunningTaskGroups.forEach(taskGroupId ->
            RuntimeTestUtil.sendTaskGroupStateEventToScheduler(scheduler, executorRegistry,
                taskGroupId, TaskGroupState.State.COMPLETE, 1));

        a3RunningTaskGroups.forEach(taskGroupId ->
            RuntimeTestUtil.sendTaskGroupStateEventToScheduler(scheduler, executorRegistry,
                taskGroupId, TaskGroupState.State.COMPLETE, 1));
      }
    }
    assertTrue(jobStateManager.checkJobTermination());

    RuntimeTestUtil.cleanup();
  }
}
