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
package edu.snu.nemo.runtime.master;

import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.local.LocalMessageDispatcher;
import edu.snu.nemo.runtime.common.message.local.LocalMessageEnvironment;
import edu.snu.nemo.runtime.common.plan.PhysicalPlan;
import edu.snu.nemo.runtime.common.plan.Stage;
import edu.snu.nemo.runtime.common.state.PlanState;
import edu.snu.nemo.runtime.common.state.StageState;
import edu.snu.nemo.runtime.common.state.TaskState;
import edu.snu.nemo.runtime.common.plan.TestPlanGenerator;
import org.apache.reef.tang.Injector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link PlanStateManager}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(MetricMessageHandler.class)
public final class PlanStateManagerTest {
  private static final int MAX_SCHEDULE_ATTEMPT = 2;
  private MetricMessageHandler metricMessageHandler;

  @Before
  public void setUp() throws Exception {
    final Injector injector = LocalMessageEnvironment.forkInjector(LocalMessageDispatcher.getInjector(),
        MessageEnvironment.MASTER_COMMUNICATION_ID);
    metricMessageHandler = mock(MetricMessageHandler.class);
    injector.bindVolatileParameter(JobConf.DAGDirectory.class, "");
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link PlanStateManager}.
   * State changes are explicitly called to check whether states are managed correctly or not.
   */
  @Test
  public void testPhysicalPlanStateChanges() throws Exception {
    final PhysicalPlan physicalPlan =
        TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, false);
    final PlanStateManager planStateManager =
        new PlanStateManager(physicalPlan, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);

    assertEquals(planStateManager.getPlanId(), "TestPlan");

    final List<Stage> stageList = physicalPlan.getStageDAG().getTopologicalSort();

    for (int stageIdx = 0; stageIdx < stageList.size(); stageIdx++) {
      final Stage stage = stageList.get(stageIdx);
      final List<String> taskIds = stage.getTaskIds();
      taskIds.forEach(taskId -> {
        planStateManager.onTaskStateChanged(taskId, TaskState.State.EXECUTING);
        planStateManager.onTaskStateChanged(taskId, TaskState.State.COMPLETE);
        if (RuntimeIdGenerator.getIndexFromTaskId(taskId) == taskIds.size() - 1) {
          assertEquals(StageState.State.COMPLETE, planStateManager.getStageState(stage.getId()));
        }
      });
      taskIds.forEach(taskId -> assertEquals(planStateManager.getTaskState(taskId), TaskState.State.COMPLETE));

      if (stageIdx == stageList.size() - 1) {
        assertEquals(planStateManager.getPlanState(), PlanState.State.COMPLETE);
      }
    }
  }

  /**
   * Test whether the methods waiting for the finish of the plan works properly.
   */
  @Test(timeout = 2000)
  public void testWaitUntilFinish() throws Exception {
    final PhysicalPlan physicalPlan =
        TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, false);
    final PlanStateManager planStateManager =
        new PlanStateManager(physicalPlan, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);

    assertFalse(planStateManager.isPlanDone());

    // Wait for the plan to finish and check the plan state.
    // It have to return EXECUTING state after timeout.
    final PlanState.State executingState = planStateManager.waitUntilFinish(100, TimeUnit.MILLISECONDS);
    assertEquals(PlanState.State.EXECUTING, executingState);

    // Complete the plan and check the result again.
    // It has to return COMPLETE.
    final List<String> tasks = physicalPlan.getStageDAG().getTopologicalSort().stream()
        .flatMap(stage -> stage.getTaskIds().stream())
        .collect(Collectors.toList());
    tasks.forEach(taskId -> planStateManager.onTaskStateChanged(taskId, TaskState.State.EXECUTING));
    tasks.forEach(taskId -> planStateManager.onTaskStateChanged(taskId, TaskState.State.COMPLETE));
    final PlanState.State completedState = planStateManager.waitUntilFinish();
    assertEquals(PlanState.State.COMPLETE, completedState);
  }
}
