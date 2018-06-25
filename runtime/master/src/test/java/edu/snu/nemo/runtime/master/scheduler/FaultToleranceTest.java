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

import edu.snu.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.message.MessageSender;
import edu.snu.nemo.runtime.common.plan.PhysicalPlan;
import edu.snu.nemo.runtime.common.plan.Stage;
import edu.snu.nemo.runtime.common.state.TaskState;
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

  private SchedulingPolicy schedulingPolicy;
  private SchedulerRunner schedulerRunner;
  private ExecutorRegistry executorRegistry;

  private MetricMessageHandler metricMessageHandler;
  private PendingTaskCollectionPointer pendingTaskCollectionPointer;
  private PubSubEventHandlerWrapper pubSubEventHandler;
  private UpdatePhysicalPlanEventHandler updatePhysicalPlanEventHandler;
  private BlockManagerMaster blockManagerMaster = mock(BlockManagerMaster.class);
  private final MessageSender<ControlMessage.Message> mockMsgSender = mock(MessageSender.class);
  private final ExecutorService serExecutorService = Executors.newSingleThreadExecutor();

  private static final int MAX_SCHEDULE_ATTEMPT = Integer.MAX_VALUE;

  @Before
  public void setUp() throws Exception {
    metricMessageHandler = mock(MetricMessageHandler.class);
    pubSubEventHandler = mock(PubSubEventHandlerWrapper.class);
    updatePhysicalPlanEventHandler = mock(UpdatePhysicalPlanEventHandler.class);

  }

  private Scheduler setUpScheduler(final boolean useMockSchedulerRunner) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    executorRegistry = injector.getInstance(ExecutorRegistry.class);

    pendingTaskCollectionPointer = new PendingTaskCollectionPointer();
    schedulingPolicy = injector.getInstance(CompositeSchedulingPolicy.class);

    if (useMockSchedulerRunner) {
      schedulerRunner = mock(SchedulerRunner.class);
    } else {
      schedulerRunner = new SchedulerRunner(schedulingPolicy, pendingTaskCollectionPointer, executorRegistry);
    }
    return new BatchSingleJobScheduler(schedulerRunner, pendingTaskCollectionPointer, blockManagerMaster,
        pubSubEventHandler, updatePhysicalPlanEventHandler, executorRegistry);
  }

  /**
   * Tests fault tolerance after a container removal.
   */
  @Test(timeout=5000)
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

    final Scheduler scheduler = setUpScheduler(true);
    for (final ExecutorRepresenter executor : executors) {
      scheduler.onExecutorAdded(executor);
    }

    final PhysicalPlan plan =
        TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, false);

    final JobStateManager jobStateManager =
        new JobStateManager(plan, blockManagerMaster, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);
    scheduler.scheduleJob(plan, jobStateManager);

    final List<Stage> dagOf4Stages = plan.getStageDAG().getTopologicalSort();

    for (final Stage stage : dagOf4Stages) {
      if (stage.getScheduleGroupIndex() == 0) {

        // There are 3 executors, each of capacity 2, and there are 6 Tasks in ScheduleGroup 0.
        SchedulerTestUtil.mockSchedulingBySchedulerRunner(pendingTaskCollectionPointer, schedulingPolicy, jobStateManager,
            executorRegistry, false);
        stage.getTaskIds().forEach(taskId ->
            SchedulerTestUtil.sendTaskStateEventToScheduler(scheduler, executorRegistry,
                taskId, TaskState.State.COMPLETE, 1));
      } else if (stage.getScheduleGroupIndex() == 1) {
        scheduler.onExecutorRemoved("a3");
        // There are 2 executors, each of capacity 2, and there are 2 Tasks in ScheduleGroup 1.
        SchedulerTestUtil.mockSchedulingBySchedulerRunner(pendingTaskCollectionPointer, schedulingPolicy, jobStateManager,
            executorRegistry, false);

        // Due to round robin scheduling, "a2" is assured to have a running Task.
        scheduler.onExecutorRemoved("a2");

        // Re-schedule
        SchedulerTestUtil.mockSchedulingBySchedulerRunner(pendingTaskCollectionPointer, schedulingPolicy, jobStateManager,
            executorRegistry, false);

        final Optional<Integer> maxTaskAttempt = stage.getTaskIds().stream()
            .map(jobStateManager::getTaskAttempt).max(Integer::compareTo);
        assertTrue(maxTaskAttempt.isPresent());
        assertEquals(2, (int) maxTaskAttempt.get());

        SchedulerTestUtil.mockSchedulingBySchedulerRunner(pendingTaskCollectionPointer, schedulingPolicy, jobStateManager,
            executorRegistry, false);
        stage.getTaskIds().forEach(taskId ->
            SchedulerTestUtil.sendTaskStateEventToScheduler(scheduler, executorRegistry,
                taskId, TaskState.State.COMPLETE, 1));
      } else {
        // There are 1 executors, each of capacity 2, and there are 2 Tasks in ScheduleGroup 2.
        // Schedule only the first Task
        SchedulerTestUtil.mockSchedulingBySchedulerRunner(pendingTaskCollectionPointer, schedulingPolicy, jobStateManager,
            executorRegistry, true);
      }
    }
  }

  /**
   * Tests fault tolerance after an output write failure.
   */
  @Test(timeout=5000)
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
    final Scheduler scheduler = setUpScheduler(true);
    for (final ExecutorRepresenter executor : executors) {
      scheduler.onExecutorAdded(executor);
    }

    final PhysicalPlan plan =
        TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, false);
    final JobStateManager jobStateManager =
        new JobStateManager(plan, blockManagerMaster, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);
    scheduler.scheduleJob(plan, jobStateManager);

    final List<Stage> dagOf4Stages = plan.getStageDAG().getTopologicalSort();

    for (final Stage stage : dagOf4Stages) {
      if (stage.getScheduleGroupIndex() == 0) {

        // There are 3 executors, each of capacity 2, and there are 6 Tasks in ScheduleGroup 0.
        SchedulerTestUtil.mockSchedulingBySchedulerRunner(pendingTaskCollectionPointer, schedulingPolicy, jobStateManager,
            executorRegistry, false);
        stage.getTaskIds().forEach(taskId ->
            SchedulerTestUtil.sendTaskStateEventToScheduler(scheduler, executorRegistry,
                taskId, TaskState.State.COMPLETE, 1));
      } else if (stage.getScheduleGroupIndex() == 1) {
        // There are 3 executors, each of capacity 2, and there are 2 Tasks in ScheduleGroup 1.
        SchedulerTestUtil.mockSchedulingBySchedulerRunner(pendingTaskCollectionPointer, schedulingPolicy, jobStateManager,
            executorRegistry, false);
        stage.getTaskIds().forEach(taskId ->
            SchedulerTestUtil.sendTaskStateEventToScheduler(scheduler, executorRegistry,
                taskId, TaskState.State.FAILED_RECOVERABLE, 1,
                TaskState.RecoverableFailureCause.OUTPUT_WRITE_FAILURE));

        // Re-schedule
        SchedulerTestUtil.mockSchedulingBySchedulerRunner(pendingTaskCollectionPointer, schedulingPolicy, jobStateManager,
            executorRegistry, false);

        final Optional<Integer> maxTaskAttempt = stage.getTaskIds().stream()
            .map(jobStateManager::getTaskAttempt).max(Integer::compareTo);
        assertTrue(maxTaskAttempt.isPresent());
        assertEquals(2, (int) maxTaskAttempt.get());

        stage.getTaskIds().forEach(taskId ->
            assertEquals(TaskState.State.EXECUTING, jobStateManager.getTaskState(taskId)));
      }
    }
  }

  /**
   * Tests fault tolerance after an input read failure.
   */
  @Test(timeout=5000)
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
    final Scheduler scheduler = setUpScheduler(true);
    for (final ExecutorRepresenter executor : executors) {
      scheduler.onExecutorAdded(executor);
    }

    final PhysicalPlan plan =
        TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, false);
    final JobStateManager jobStateManager =
        new JobStateManager(plan, blockManagerMaster, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);
    scheduler.scheduleJob(plan, jobStateManager);

    final List<Stage> dagOf4Stages = plan.getStageDAG().getTopologicalSort();

    for (final Stage stage : dagOf4Stages) {
      if (stage.getScheduleGroupIndex() == 0) {

        // There are 3 executors, each of capacity 2, and there are 6 Tasks in ScheduleGroup 0.
        SchedulerTestUtil.mockSchedulingBySchedulerRunner(pendingTaskCollectionPointer, schedulingPolicy, jobStateManager,
            executorRegistry, false);
        stage.getTaskIds().forEach(taskId ->
            SchedulerTestUtil.sendTaskStateEventToScheduler(scheduler, executorRegistry,
                taskId, TaskState.State.COMPLETE, 1));
      } else if (stage.getScheduleGroupIndex() == 1) {
        // There are 3 executors, each of capacity 2, and there are 2 Tasks in ScheduleGroup 1.
        SchedulerTestUtil.mockSchedulingBySchedulerRunner(pendingTaskCollectionPointer, schedulingPolicy, jobStateManager,
            executorRegistry, false);

        stage.getTaskIds().forEach(taskId ->
            SchedulerTestUtil.sendTaskStateEventToScheduler(scheduler, executorRegistry,
                taskId, TaskState.State.FAILED_RECOVERABLE, 1,
                TaskState.RecoverableFailureCause.INPUT_READ_FAILURE));

        // Re-schedule
        SchedulerTestUtil.mockSchedulingBySchedulerRunner(pendingTaskCollectionPointer, schedulingPolicy, jobStateManager,
            executorRegistry, false);

        final Optional<Integer> maxTaskAttempt = stage.getTaskIds().stream()
            .map(jobStateManager::getTaskAttempt).max(Integer::compareTo);
        assertTrue(maxTaskAttempt.isPresent());
        assertEquals(2, (int) maxTaskAttempt.get());

        stage.getTaskIds().forEach(taskId ->
            assertEquals(TaskState.State.EXECUTING, jobStateManager.getTaskState(taskId)));
      }
    }
  }

  /**
   * Tests the rescheduling of Tasks upon a failure.
   */
  @Test(timeout=20000)
  public void testTaskReexecutionForFailure() throws Exception {
    final ActiveContext activeContext = mock(ActiveContext.class);
    Mockito.doThrow(new RuntimeException()).when(activeContext).close();

    final ResourceSpecification computeSpec = new ResourceSpecification(ExecutorPlacementProperty.COMPUTE, 2, 0);
    final Function<String, ExecutorRepresenter> executorRepresenterGenerator = executorId ->
        new ExecutorRepresenter(executorId, computeSpec, mockMsgSender, activeContext, serExecutorService, executorId);

    final Scheduler scheduler = setUpScheduler(false);
    final PhysicalPlan plan =
        TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, false);
    final JobStateManager jobStateManager =
        new JobStateManager(plan, blockManagerMaster, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);
    scheduler.scheduleJob(plan, jobStateManager);

    final List<ExecutorRepresenter> executors = new ArrayList<>();
    final List<Stage> dagOf4Stages = plan.getStageDAG().getTopologicalSort();

    int executorIdIndex = 1;
    float removalChance = 0.5f; // Out of 1.0
    final Random random = new Random(0); // Deterministic seed.

    for (final Stage stage : dagOf4Stages) {

      while (jobStateManager.getStageState(stage.getId()) != COMPLETE) {
        // By chance, remove or add executor
        if (isTrueByChance(random, removalChance)) {
          // REMOVE EXECUTOR
          if (!executors.isEmpty()) {
            scheduler.onExecutorRemoved(executors.remove(random.nextInt(executors.size())).getExecutorId());
          } else {
            // Skip, since no executor is running.
          }
        } else {
          if (executors.size() < 3) {
            // ADD EXECUTOR
            final ExecutorRepresenter newExecutor = executorRepresenterGenerator.apply("a" + executorIdIndex);
            executorIdIndex += 1;
            executors.add(newExecutor);
            scheduler.onExecutorAdded(newExecutor);
          } else {
            // Skip, in order to keep the total number of running executors below or equal to 3
          }
        }

        // Complete the execution of tasks
        if (!executors.isEmpty()) {
          final int indexOfCompletedExecutor = random.nextInt(executors.size());
          // New set for snapshotting
          final Map<String, Integer> runningTaskSnapshot =
              new HashMap<>(executors.get(indexOfCompletedExecutor).getRunningTaskToAttempt());
          runningTaskSnapshot.entrySet().forEach(entry -> {
            SchedulerTestUtil.sendTaskStateEventToScheduler(
                scheduler, executorRegistry, entry.getKey(), TaskState.State.COMPLETE, entry.getValue());
          });
        }
      }
    }
    assertTrue(jobStateManager.isJobDone());
  }

  private boolean isTrueByChance(final Random random, final float chance) {
    return chance > random.nextDouble();
  }
}
