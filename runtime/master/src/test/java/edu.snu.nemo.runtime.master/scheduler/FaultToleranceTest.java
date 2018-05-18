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

import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
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
import edu.snu.nemo.runtime.plangenerator.TestPlanGenerator;
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
  private PendingTaskGroupCollection pendingTaskGroupCollection;
  private PubSubEventHandlerWrapper pubSubEventHandler;
  private UpdatePhysicalPlanEventHandler updatePhysicalPlanEventHandler;
  private BlockManagerMaster blockManagerMaster = mock(BlockManagerMaster.class);
  private final MessageSender<ControlMessage.Message> mockMsgSender = mock(MessageSender.class);
  private final ExecutorService serExecutorService = Executors.newSingleThreadExecutor();

  private static final int MAX_SCHEDULE_ATTEMPT = 3;

  @Before
  public void setUp() throws Exception {
    irDAGBuilder = new DAGBuilder<>();

    metricMessageHandler = mock(MetricMessageHandler.class);
    pubSubEventHandler = mock(PubSubEventHandlerWrapper.class);
    updatePhysicalPlanEventHandler = mock(UpdatePhysicalPlanEventHandler.class);

  }

  private void setUpExecutors(final Collection<ExecutorRepresenter> executors,
                              final boolean useMockSchedulerRunner) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    executorRegistry = injector.getInstance(ExecutorRegistry.class);

    pendingTaskGroupCollection = new SingleJobTaskGroupCollection();
    schedulingPolicy = injector.getInstance(CompositeSchedulingPolicy.class);

    if (useMockSchedulerRunner) {
      schedulerRunner = mock(SchedulerRunner.class);
    } else {
      schedulerRunner = new SchedulerRunner(schedulingPolicy, pendingTaskGroupCollection, executorRegistry);
    }
    scheduler =
        new BatchSingleJobScheduler(schedulingPolicy, schedulerRunner, pendingTaskGroupCollection,
            blockManagerMaster, pubSubEventHandler, updatePhysicalPlanEventHandler, executorRegistry);

    // Add nodes
    for (final ExecutorRepresenter executor : executors) {
      scheduler.onExecutorAdded(executor);
    }
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
    final PhysicalPlan plan =
        TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, false);

    final JobStateManager jobStateManager =
        new JobStateManager(plan, blockManagerMaster, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);
    scheduler.scheduleJob(plan, jobStateManager);

    final List<PhysicalStage> dagOf4Stages = plan.getStageDAG().getTopologicalSort();

    for (final PhysicalStage stage : dagOf4Stages) {
      if (stage.getScheduleGroupIndex() == 0) {

        // There are 3 executors, each of capacity 2, and there are 6 TaskGroups in ScheduleGroup 0.
        SchedulerTestUtil.mockSchedulerRunner(pendingTaskGroupCollection, schedulingPolicy, jobStateManager,
            executorRegistry, false);
        assertTrue(pendingTaskGroupCollection.isEmpty());
        stage.getTaskGroupIds().forEach(taskGroupId ->
            SchedulerTestUtil.sendTaskGroupStateEventToScheduler(scheduler, executorRegistry,
                taskGroupId, TaskGroupState.State.COMPLETE, 1));
      } else if (stage.getScheduleGroupIndex() == 1) {
        scheduler.onExecutorRemoved("a3");
        // There are 2 executors, each of capacity 2, and there are 2 TaskGroups in ScheduleGroup 1.
        SchedulerTestUtil.mockSchedulerRunner(pendingTaskGroupCollection, schedulingPolicy, jobStateManager,
            executorRegistry, false);

        // Due to round robin scheduling, "a2" is assured to have a running TaskGroup.
        scheduler.onExecutorRemoved("a2");

        while (jobStateManager.getStageState(stage.getId()).getStateMachine().getCurrentState() != EXECUTING) {

        }
        assertEquals(jobStateManager.getAttemptCountForStage(stage.getId()), 2);

        SchedulerTestUtil.mockSchedulerRunner(pendingTaskGroupCollection, schedulingPolicy, jobStateManager,
            executorRegistry, false);
        assertTrue(pendingTaskGroupCollection.isEmpty());
        stage.getTaskGroupIds().forEach(taskGroupId ->
            SchedulerTestUtil.sendTaskGroupStateEventToScheduler(scheduler, executorRegistry,
                taskGroupId, TaskGroupState.State.COMPLETE, 1));
      } else {
        // There are 1 executors, each of capacity 2, and there are 2 TaskGroups in ScheduleGroup 2.
        // Schedule only the first TaskGroup
        SchedulerTestUtil.mockSchedulerRunner(pendingTaskGroupCollection, schedulingPolicy, jobStateManager,
            executorRegistry, true);
      }
    }
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

    final PhysicalPlan plan =
        TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, false);
    final JobStateManager jobStateManager =
        new JobStateManager(plan, blockManagerMaster, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);
    scheduler.scheduleJob(plan, jobStateManager);

    final List<PhysicalStage> dagOf4Stages = plan.getStageDAG().getTopologicalSort();

    for (final PhysicalStage stage : dagOf4Stages) {
      if (stage.getScheduleGroupIndex() == 0) {

        // There are 3 executors, each of capacity 2, and there are 6 TaskGroups in ScheduleGroup 0.
        SchedulerTestUtil.mockSchedulerRunner(pendingTaskGroupCollection, schedulingPolicy, jobStateManager,
            executorRegistry, false);
        assertTrue(pendingTaskGroupCollection.isEmpty());
        stage.getTaskGroupIds().forEach(taskGroupId ->
            SchedulerTestUtil.sendTaskGroupStateEventToScheduler(scheduler, executorRegistry,
                taskGroupId, TaskGroupState.State.COMPLETE, 1));
      } else if (stage.getScheduleGroupIndex() == 1) {
        // There are 3 executors, each of capacity 2, and there are 2 TaskGroups in ScheduleGroup 1.
        SchedulerTestUtil.mockSchedulerRunner(pendingTaskGroupCollection, schedulingPolicy, jobStateManager,
            executorRegistry, false);
        assertTrue(pendingTaskGroupCollection.isEmpty());
        stage.getTaskGroupIds().forEach(taskGroupId ->
            SchedulerTestUtil.sendTaskGroupStateEventToScheduler(scheduler, executorRegistry,
                taskGroupId, TaskGroupState.State.FAILED_RECOVERABLE, 1,
                TaskGroupState.RecoverableFailureCause.OUTPUT_WRITE_FAILURE));

        while (jobStateManager.getStageState(stage.getId()).getStateMachine().getCurrentState() != EXECUTING) {

        }

        assertEquals(jobStateManager.getAttemptCountForStage(stage.getId()), 3);
        assertFalse(pendingTaskGroupCollection.isEmpty());
        stage.getTaskGroupIds().forEach(taskGroupId -> {
          assertEquals(jobStateManager.getTaskGroupState(taskGroupId).getStateMachine().getCurrentState(),
              TaskGroupState.State.READY);
        });
      }
    }
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

    final PhysicalPlan plan =
        TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, false);
    final JobStateManager jobStateManager =
        new JobStateManager(plan, blockManagerMaster, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);
    scheduler.scheduleJob(plan, jobStateManager);

    final List<PhysicalStage> dagOf4Stages = plan.getStageDAG().getTopologicalSort();

    for (final PhysicalStage stage : dagOf4Stages) {
      if (stage.getScheduleGroupIndex() == 0) {

        // There are 3 executors, each of capacity 2, and there are 6 TaskGroups in ScheduleGroup 0.
        SchedulerTestUtil.mockSchedulerRunner(pendingTaskGroupCollection, schedulingPolicy, jobStateManager,
            executorRegistry, false);
        assertTrue(pendingTaskGroupCollection.isEmpty());
        stage.getTaskGroupIds().forEach(taskGroupId ->
            SchedulerTestUtil.sendTaskGroupStateEventToScheduler(scheduler, executorRegistry,
                taskGroupId, TaskGroupState.State.COMPLETE, 1));
      } else if (stage.getScheduleGroupIndex() == 1) {
        // There are 3 executors, each of capacity 2, and there are 2 TaskGroups in ScheduleGroup 1.
        SchedulerTestUtil.mockSchedulerRunner(pendingTaskGroupCollection, schedulingPolicy, jobStateManager,
            executorRegistry, false);

        stage.getTaskGroupIds().forEach(taskGroupId ->
            SchedulerTestUtil.sendTaskGroupStateEventToScheduler(scheduler, executorRegistry,
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

    final PhysicalPlan plan =
        TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, false);


    final JobStateManager jobStateManager =
        new JobStateManager(plan, blockManagerMaster, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);

    scheduler.scheduleJob(plan, jobStateManager);
    scheduler.onExecutorRemoved("a2");

    final List<PhysicalStage> dagOf4Stages = plan.getStageDAG().getTopologicalSort();

    for (final PhysicalStage stage : dagOf4Stages) {
      while (jobStateManager.getStageState(stage.getId()).getStateMachine().getCurrentState() != COMPLETE) {
        final Set<String> a1RunningTaskGroups = new HashSet<>(a1.getRunningTaskGroups());
        final Set<String> a3RunningTaskGroups = new HashSet<>(a3.getRunningTaskGroups());

        a1RunningTaskGroups.forEach(taskGroupId ->
            SchedulerTestUtil.sendTaskGroupStateEventToScheduler(scheduler, executorRegistry,
                taskGroupId, TaskGroupState.State.COMPLETE, 1));

        a3RunningTaskGroups.forEach(taskGroupId ->
            SchedulerTestUtil.sendTaskGroupStateEventToScheduler(scheduler, executorRegistry,
                taskGroupId, TaskGroupState.State.COMPLETE, 1));
      }
    }
    assertTrue(jobStateManager.checkJobTermination());
  }
}
