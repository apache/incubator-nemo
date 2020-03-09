/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.master.scheduler;

import org.apache.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageSender;
import org.apache.nemo.runtime.common.message.local.LocalMessageDispatcher;
import org.apache.nemo.runtime.common.message.local.LocalMessageEnvironment;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.PlanRewriter;
import org.apache.nemo.runtime.common.plan.TestPlanGenerator;
import org.apache.nemo.runtime.common.state.BlockState;
import org.apache.nemo.runtime.common.state.PlanState;
import org.apache.nemo.runtime.common.state.TaskState;
import org.apache.nemo.runtime.master.BlockManagerMaster;
import org.apache.nemo.runtime.master.PlanStateManager;
import org.apache.nemo.runtime.master.metric.MetricMessageHandler;
import org.apache.nemo.runtime.master.resource.DefaultExecutorRepresenter;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.nemo.common.ir.executionproperty.ResourceSpecification;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.tang.Injector;
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
@PrepareForTest({BlockManagerMaster.class, TaskDispatcher.class, SchedulingConstraintRegistry.class,
  PubSubEventHandlerWrapper.class})
public final class TaskRetryTest {
  @Rule
  public TestName testName = new TestName();

  private static final Logger LOG = LoggerFactory.getLogger(TaskRetryTest.class.getName());
  private static final AtomicInteger ID_OFFSET = new AtomicInteger(1);

  private Random random;
  private Scheduler scheduler;
  private ExecutorRegistry executorRegistry;
  private PlanStateManager planStateManager;
  private BlockManagerMaster blockManagerMaster;

  private static final int MAX_SCHEDULE_ATTEMPT = Integer.MAX_VALUE;

  @Before
  public void setUp() throws Exception {
    // To understand which part of the log belongs to which test
    LOG.info("===== Testing {} =====", testName.getMethodName());
    final Injector injector = LocalMessageEnvironment.forkInjector(LocalMessageDispatcher.getInjector(),
      MessageEnvironment.MASTER_COMMUNICATION_ID);

    // Get random
    random = new Random(0); // Fixed seed for reproducing test results.

    // Get executorRegistry
    executorRegistry = injector.getInstance(ExecutorRegistry.class);

    // Get PlanStateManager
    runPhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, injector);
  }

  @Test(timeout = 60000)
  public void testExecutorRemoved() throws Exception {
    // Until the plan finishes, events happen
    while (!planStateManager.isPlanDone()) {
      // 30% chance executor added, 30% chance executor removed
      executorAdded(0.3);
      executorRemoved(0.3);

      // random - trigger speculative execution.
      if (random.nextBoolean()) {
        Thread.sleep(10);
      } else {
        Thread.sleep(20);
      }

      // 30% chance task completed,
      taskCompleted(0.3);

      scheduler.onSpeculativeExecutionCheck();
    }

    // Plan should COMPLETE
    assertEquals(PlanState.State.COMPLETE, planStateManager.getPlanState());
    assertTrue(planStateManager.isPlanDone());
  }

  @Test(timeout = 60000)
  public void testTaskOutputWriteFailure() throws Exception {
    // Three executors are used
    executorAdded(1.0);
    executorAdded(1.0);
    executorAdded(1.0);

    // Until the plan finishes, events happen
    while (!planStateManager.isPlanDone()) {
      // 50% chance task completed
      // 70% chance task output write failed
      taskCompleted(0.5);
      taskOutputWriteFailed(0.7);

      // random - trigger speculative execution.
      if (random.nextBoolean()) {
        Thread.sleep(10);
      } else {
        Thread.sleep(20);
      }
      scheduler.onSpeculativeExecutionCheck();
    }

    // Plan should COMPLETE
    assertEquals(PlanState.State.COMPLETE, planStateManager.getPlanState());
    assertTrue(planStateManager.isPlanDone());
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
    final ResourceSpecification computeSpec = new ResourceSpecification(ResourcePriorityProperty.COMPUTE, 2, 0);
    final ExecutorRepresenter executor = new DefaultExecutorRepresenter("EXECUTOR" + ID_OFFSET.getAndIncrement(),
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

    final List<String> executingTasks = getTasksInState(planStateManager, TaskState.State.EXECUTING);
    if (!executingTasks.isEmpty()) {
      final int randomIndex = random.nextInt(executingTasks.size());
      final String selectedTask = executingTasks.get(randomIndex);


      final Optional<ExecutorRepresenter> executor = executorRegistry.findExecutorForTask(selectedTask);
      if (executor.isPresent()) {
        SchedulerTestUtil.sendTaskStateEventToScheduler(scheduler, executorRegistry, selectedTask,
          TaskState.State.COMPLETE, RuntimeIdManager.getAttemptFromTaskId(selectedTask));
        getOutputBlockIds(selectedTask).forEach(blockId ->
          blockManagerMaster.onBlockStateChanged(blockId, BlockState.State.AVAILABLE, executor.get().getExecutorId()));
      } else {
        throw new RuntimeException(selectedTask);
      }
    }
  }

  private void taskOutputWriteFailed(final double chance) {
    if (random.nextDouble() > chance) {
      return;
    }

    final List<String> executingTasks = getTasksInState(planStateManager, TaskState.State.EXECUTING);
    if (!executingTasks.isEmpty()) {
      final int randomIndex = random.nextInt(executingTasks.size());
      final String selectedTask = executingTasks.get(randomIndex);
      SchedulerTestUtil.sendTaskStateEventToScheduler(scheduler, executorRegistry, selectedTask,
        TaskState.State.SHOULD_RETRY, RuntimeIdManager.getAttemptFromTaskId(selectedTask),
        TaskState.RecoverableTaskFailureCause.OUTPUT_WRITE_FAILURE);
    }
  }

  ////////////////////////////////////////////////////////////////// Helper methods

  private List<String> getTasksInState(final PlanStateManager planStateManager, final TaskState.State state) {
    return planStateManager.getAllTaskAttemptIdsToItsState()
      .entrySet()
      .stream()
      .filter(entry -> entry.getValue().equals(state))
      .map(Map.Entry::getKey)
      .collect(Collectors.toList());
  }

  private void runPhysicalPlan(final TestPlanGenerator.PlanType planType,
                               final Injector injector) throws Exception {
    final MetricMessageHandler metricMessageHandler = mock(MetricMessageHandler.class);
    final PlanRewriter planRewriter = mock(PlanRewriter.class);
    final PhysicalPlan plan = TestPlanGenerator.generatePhysicalPlan(planType, false);

    // Get scheduler
    injector.bindVolatileInstance(MetricMessageHandler.class, metricMessageHandler);
    injector.bindVolatileInstance(PubSubEventHandlerWrapper.class, mock(PubSubEventHandlerWrapper.class));
    injector.bindVolatileInstance(SchedulingConstraintRegistry.class, mock(SchedulingConstraintRegistry.class));
    injector.bindVolatileInstance(PlanRewriter.class, planRewriter);
    planStateManager = injector.getInstance(PlanStateManager.class);
    scheduler = injector.getInstance(BatchScheduler.class);
    blockManagerMaster = injector.getInstance(BlockManagerMaster.class);

    scheduler.schedulePlan(plan, MAX_SCHEDULE_ATTEMPT);
  }

  private Set<String> getOutputBlockIds(final String taskId) {
    return planStateManager.getPhysicalPlan().getStageDAG()
      .getOutgoingEdgesOf(RuntimeIdManager.getStageIdFromTaskId(taskId))
      .stream()
      .map(stageEdge -> RuntimeIdManager.generateBlockId(stageEdge.getId(), taskId))
      .collect(Collectors.toSet()); // ids of blocks this task will produce
  }
}
