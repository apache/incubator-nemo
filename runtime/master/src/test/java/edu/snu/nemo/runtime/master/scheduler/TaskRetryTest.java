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
import edu.snu.nemo.runtime.common.state.JobState;
import edu.snu.nemo.runtime.common.state.TaskState;
import edu.snu.nemo.runtime.master.BlockManagerMaster;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.master.MetricMessageHandler;
import edu.snu.nemo.runtime.master.eventhandler.UpdatePhysicalPlanEventHandler;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import edu.snu.nemo.runtime.master.resource.ResourceSpecification;
import edu.snu.nemo.runtime.common.plan.TestPlanGenerator;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests fault tolerance.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({BlockManagerMaster.class, SchedulerRunner.class, SchedulingConstraintRegistry.class,
    PubSubEventHandlerWrapper.class, UpdatePhysicalPlanEventHandler.class, MetricMessageHandler.class})
public final class TaskRetryTest {
  @Rule public TestName testName = new TestName();

  private static final Logger LOG = LoggerFactory.getLogger(TaskRetryTest.class.getName());
  private static final AtomicInteger ID_OFFSET = new AtomicInteger(1);

  private Random random;
  private Scheduler scheduler;
  private ExecutorRegistry executorRegistry;
  private JobStateManager jobStateManager;

  private static final int MAX_SCHEDULE_ATTEMPT = Integer.MAX_VALUE;

  @Before
  public void setUp() throws Exception {
    // To understand which part of the log belongs to which test
    LOG.info("===== Testing {} =====", testName.getMethodName());
    final Injector injector = Tang.Factory.getTang().newInjector();

    // Get random
    random = new Random(0); // Fixed seed for reproducing test results.

    // Get executorRegistry
    executorRegistry = injector.getInstance(ExecutorRegistry.class);

    // Get scheduler
    injector.bindVolatileInstance(PubSubEventHandlerWrapper.class, mock(PubSubEventHandlerWrapper.class));
    injector.bindVolatileInstance(UpdatePhysicalPlanEventHandler.class, mock(UpdatePhysicalPlanEventHandler.class));
    injector.bindVolatileInstance(SchedulingConstraintRegistry.class, mock(SchedulingConstraintRegistry.class));
    injector.bindVolatileInstance(BlockManagerMaster.class, mock(BlockManagerMaster.class));
    scheduler = injector.getInstance(Scheduler.class);

    // Get JobStateManager
    jobStateManager = runPhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined);
  }

  @Test(timeout=7000)
  public void testExecutorRemoved() throws Exception {
    // Until the job finishes, events happen
    while (!jobStateManager.isJobDone()) {
      // 50% chance remove, 50% chance add, 80% chance task completed
      executorRemoved(0.5);
      executorAdded(0.5);
      taskCompleted(0.8);

      // 10ms sleep
      Thread.sleep(10);
    }

    // Job should COMPLETE
    assertEquals(JobState.State.COMPLETE, jobStateManager.getJobState());
    assertTrue(jobStateManager.isJobDone());
  }

  @Test(timeout=7000)
  public void testTaskOutputWriteFailure() throws Exception {
    // Three executors are used
    executorAdded(1.0);
    executorAdded(1.0);
    executorAdded(1.0);

    // Until the job finishes, events happen
    while (!jobStateManager.isJobDone()) {
      // 50% chance task completed
      // 50% chance task output write failed
      taskCompleted(0.5);
      taskOutputWriteFailed(0.5);

      // 10ms sleep
      Thread.sleep(10);
    }

    // Job should COMPLETE
    assertEquals(JobState.State.COMPLETE, jobStateManager.getJobState());
    assertTrue(jobStateManager.isJobDone());
  }

  ////////////////////////////////////////////////////////////////// Events

  private void executorAdded(final double chance) {
    if (random.nextDouble() > chance) {
      return;
    }

    final MessageSender<ControlMessage.Message> mockMsgSender = mock(MessageSender.class);
    final ActiveContext activeContext = mock(ActiveContext.class);
    Mockito.doThrow(new RuntimeException()).when(activeContext).close();
    final ExecutorService serExecutorService = Executors.newSingleThreadExecutor();
    final ResourceSpecification computeSpec = new ResourceSpecification(ExecutorPlacementProperty.COMPUTE, 2, 0);
    final ExecutorRepresenter executor = new ExecutorRepresenter("EXECUTOR" + ID_OFFSET.getAndIncrement(),
        computeSpec, mockMsgSender, activeContext, serExecutorService, "NODE" + ID_OFFSET.getAndIncrement());
    scheduler.onExecutorAdded(executor);
  }

  private void executorRemoved(final double chance) {
    if (random.nextDouble() > chance) {
      return;
    }

    executorRegistry.viewExecutors(executors -> {
      if (executors.isEmpty()) {
        return;
      }

      final List<ExecutorRepresenter> executorList = new ArrayList<>(executors);
      final int randomIndex = random.nextInt(executorList.size());

      // Because synchronized blocks are reentrant and there's no additional operation after this point,
      // we can scheduler.onExecutorRemoved() while being inside executorRegistry.viewExecutors()
      scheduler.onExecutorRemoved(executorList.get(randomIndex).getExecutorId());
    });
  }

  private void taskCompleted(final double chance) {
    if (random.nextDouble() > chance) {
      return;
    }

    final List<String> executingTasks = getTasksInState(jobStateManager, TaskState.State.EXECUTING);
    if (!executingTasks.isEmpty()) {
      final int randomIndex = random.nextInt(executingTasks.size());
      final String selectedTask = executingTasks.get(randomIndex);
      SchedulerTestUtil.sendTaskStateEventToScheduler(scheduler, executorRegistry, selectedTask,
          TaskState.State.COMPLETE, jobStateManager.getTaskAttempt(selectedTask));
    }
  }

  private void taskOutputWriteFailed(final double chance) {
    if (random.nextDouble() > chance) {
      return;
    }

    final List<String> executingTasks = getTasksInState(jobStateManager, TaskState.State.EXECUTING);
    if (!executingTasks.isEmpty()) {
      final int randomIndex = random.nextInt(executingTasks.size());
      final String selectedTask = executingTasks.get(randomIndex);
      SchedulerTestUtil.sendTaskStateEventToScheduler(scheduler, executorRegistry, selectedTask,
          TaskState.State.SHOULD_RETRY, jobStateManager.getTaskAttempt(selectedTask),
          TaskState.RecoverableTaskFailureCause.OUTPUT_WRITE_FAILURE);
    }
  }

  ////////////////////////////////////////////////////////////////// Helper methods

  private List<String> getTasksInState(final JobStateManager jobStateManager, final TaskState.State state) {
    return jobStateManager.getAllTaskStates().entrySet().stream()
        .filter(entry -> entry.getValue().getStateMachine().getCurrentState().equals(state))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
  }

  private JobStateManager runPhysicalPlan(final TestPlanGenerator.PlanType planType) throws Exception {
    final MetricMessageHandler metricMessageHandler = mock(MetricMessageHandler.class);
    final PhysicalPlan plan = TestPlanGenerator.generatePhysicalPlan(planType, false);
    final JobStateManager jobStateManager = new JobStateManager(plan, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);
    scheduler.scheduleJob(plan, jobStateManager);
    return jobStateManager;
  }
}
