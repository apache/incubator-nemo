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

import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.local.LocalMessageDispatcher;
import edu.snu.nemo.runtime.common.message.local.LocalMessageEnvironment;
import edu.snu.nemo.runtime.common.plan.PhysicalPlan;
import edu.snu.nemo.runtime.common.plan.PhysicalPlanGenerator;
import edu.snu.nemo.runtime.common.plan.Stage;
import edu.snu.nemo.runtime.common.plan.StageEdge;
import edu.snu.nemo.runtime.common.state.JobState;
import edu.snu.nemo.runtime.common.state.StageState;
import edu.snu.nemo.runtime.common.state.TaskState;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.runtime.plangenerator.TestPlanGenerator;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static junit.framework.TestCase.assertTrue;
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
  private DAGBuilder<IRVertex, IREdge> irDAGBuilder;
  private BlockManagerMaster blockManagerMaster;
  private MetricMessageHandler metricMessageHandler;
  private PhysicalPlanGenerator physicalPlanGenerator;

  @Before
  public void setUp() throws Exception {
    irDAGBuilder = new DAGBuilder<>();
    final LocalMessageDispatcher messageDispatcher = new LocalMessageDispatcher();
    final LocalMessageEnvironment messageEnvironment =
        new LocalMessageEnvironment(MessageEnvironment.MASTER_COMMUNICATION_ID, messageDispatcher);
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(MessageEnvironment.class, messageEnvironment);
    blockManagerMaster = injector.getInstance(BlockManagerMaster.class);
    metricMessageHandler = mock(MetricMessageHandler.class);
    injector.bindVolatileParameter(JobConf.DAGDirectory.class, "");
    physicalPlanGenerator = injector.getInstance(PhysicalPlanGenerator.class);
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
        new JobStateManager(physicalPlan, blockManagerMaster, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);

    assertEquals(jobStateManager.getJobId(), "TestPlan");

    final List<Stage> stageList = physicalPlan.getStageDAG().getTopologicalSort();

    for (int stageIdx = 0; stageIdx < stageList.size(); stageIdx++) {
      final Stage stage = stageList.get(stageIdx);
      jobStateManager.onStageStateChanged(stage.getId(), StageState.State.EXECUTING);
      final List<String> taskIds = stage.getTaskIds();
      taskIds.forEach(taskId -> {
        jobStateManager.onTaskStateChanged(taskId, TaskState.State.EXECUTING);
        jobStateManager.onTaskStateChanged(taskId, TaskState.State.COMPLETE);
        if (RuntimeIdGenerator.getIndexFromTaskId(taskId) == taskIds.size() - 1) {
          assertTrue(jobStateManager.checkStageCompletion(stage.getId()));
        }
      });
      final Map<String, TaskState> taskStateMap = jobStateManager.getIdToTaskStates();
      taskIds.forEach(taskId -> {
        assertEquals(taskStateMap.get(taskId).getStateMachine().getCurrentState(),
            TaskState.State.COMPLETE);
      });

      if (stageIdx == stageList.size() - 1) {
        assertEquals(jobStateManager.getJobState().getStateMachine().getCurrentState(), JobState.State.COMPLETE);
      }
    }
  }

  /**
   * Test whether the methods waiting finish of job works properly.
   */
  @Test(timeout = 1000)
  public void testWaitUntilFinish() {
    // Create a JobStateManager of an empty dag.
    final DAG<IRVertex, IREdge> irDAG = irDAGBuilder.build();
    final DAG<Stage, StageEdge> physicalDAG = irDAG.convert(physicalPlanGenerator);
    final JobStateManager jobStateManager = new JobStateManager(
        new PhysicalPlan("TestPlan", physicalDAG, physicalPlanGenerator.getIdToIRVertex()),
        blockManagerMaster, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);

    assertFalse(jobStateManager.checkJobTermination());

    // Wait for the job to finish and check the job state.
    // It have to return EXECUTING state after timeout.
    JobState state = jobStateManager.waitUntilFinish(100, TimeUnit.MILLISECONDS);
    assertEquals(state.getStateMachine().getCurrentState(), JobState.State.EXECUTING);

    // Complete the job and check the result again.
    // It have to return COMPLETE.
    jobStateManager.onJobStateChanged(JobState.State.COMPLETE);
    state = jobStateManager.waitUntilFinish();
    assertEquals(state.getStateMachine().getCurrentState(), JobState.State.COMPLETE);
  }
}
