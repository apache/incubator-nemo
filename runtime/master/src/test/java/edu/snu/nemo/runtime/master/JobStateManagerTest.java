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
import edu.snu.nemo.runtime.common.state.JobState;
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
 * Tests {@link JobStateManager}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(MetricMessageHandler.class)
public final class JobStateManagerTest {
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
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link JobStateManager}.
   * State changes are explicitly called to check whether states are managed correctly or not.
   */
  @Test
  public void testPhysicalPlanStateChanges() throws Exception {
    final PhysicalPlan physicalPlan =
        TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, false);
    final JobStateManager jobStateManager =
        new JobStateManager(physicalPlan, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);

    assertEquals(jobStateManager.getJobId(), "TestPlan");

    final List<Stage> stageList = physicalPlan.getStageDAG().getTopologicalSort();

    for (int stageIdx = 0; stageIdx < stageList.size(); stageIdx++) {
      final Stage stage = stageList.get(stageIdx);
      final List<String> taskIds = stage.getTaskIds();
      taskIds.forEach(taskId -> {
        jobStateManager.onTaskStateChanged(taskId, TaskState.State.EXECUTING);
        jobStateManager.onTaskStateChanged(taskId, TaskState.State.COMPLETE);
        if (RuntimeIdGenerator.getIndexFromTaskId(taskId) == taskIds.size() - 1) {
          assertEquals(StageState.State.COMPLETE, jobStateManager.getStageState(stage.getId()));
        }
      });
      taskIds.forEach(taskId -> assertEquals(jobStateManager.getTaskState(taskId), TaskState.State.COMPLETE));

      if (stageIdx == stageList.size() - 1) {
        assertEquals(jobStateManager.getJobState(), JobState.State.COMPLETE);
      }
    }
  }

  /**
   * Test whether the methods waiting finish of job works properly.
   */
  @Test(timeout = 2000)
  public void testWaitUntilFinish() throws Exception {
    final PhysicalPlan physicalPlan =
        TestPlanGenerator.generatePhysicalPlan(TestPlanGenerator.PlanType.TwoVerticesJoined, false);
    final JobStateManager jobStateManager =
        new JobStateManager(physicalPlan, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);

    assertFalse(jobStateManager.isJobDone());

    // Wait for the job to finish and check the job state.
    // It have to return EXECUTING state after timeout.
    final JobState.State executingState = jobStateManager.waitUntilFinish(100, TimeUnit.MILLISECONDS);
    assertEquals(JobState.State.EXECUTING, executingState);

    // Complete the job and check the result again.
    // It have to return COMPLETE.
    final List<String> tasks = physicalPlan.getStageDAG().getTopologicalSort().stream()
        .flatMap(stage -> stage.getTaskIds().stream())
        .collect(Collectors.toList());
    tasks.forEach(taskId -> jobStateManager.onTaskStateChanged(taskId, TaskState.State.EXECUTING));
    tasks.forEach(taskId -> jobStateManager.onTaskStateChanged(taskId, TaskState.State.COMPLETE));
    final JobState.State completedState = jobStateManager.waitUntilFinish();
    assertEquals(JobState.State.COMPLETE, completedState);
  }
}
