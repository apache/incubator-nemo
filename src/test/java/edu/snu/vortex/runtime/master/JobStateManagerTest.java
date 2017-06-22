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
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.compiler.frontend.Coder;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.OperatorVertex;
import edu.snu.vortex.compiler.ir.Transform;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.common.plan.logical.LogicalDAGGenerator;
import edu.snu.vortex.runtime.common.plan.logical.Stage;
import edu.snu.vortex.runtime.common.plan.logical.StageEdge;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.runtime.common.state.JobState;
import edu.snu.vortex.runtime.common.state.StageState;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.runtime.common.state.TaskState;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.common.dag.DAGBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link JobStateManager}.
 */
public final class JobStateManagerTest {
  private DAGBuilder<IRVertex, IREdge> irDAGBuilder;

  @Before
  public void setUp() {
    irDAGBuilder = new DAGBuilder<>();
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link JobStateManager}.
   * State changes are explicitly called to check whether states are managed correctly or not.
   */
  @Test
  public void testPhysicalPlanStateChanges() {
    final Transform t = mock(Transform.class);
    final IRVertex v1 = new OperatorVertex(t);
    v1.setAttr(Attribute.IntegerKey.Parallelism, 3);
    irDAGBuilder.addVertex(v1);

    final IRVertex v2 = new OperatorVertex(t);
    v2.setAttr(Attribute.IntegerKey.Parallelism, 2);
    irDAGBuilder.addVertex(v2);

    final IRVertex v3 = new OperatorVertex(t);
    v3.setAttr(Attribute.IntegerKey.Parallelism, 3);
    irDAGBuilder.addVertex(v3);

    final IRVertex v4 = new OperatorVertex(t);
    v4.setAttr(Attribute.IntegerKey.Parallelism, 2);
    irDAGBuilder.addVertex(v4);

    final IRVertex v5 = new OperatorVertex(t);
    v5.setAttr(Attribute.IntegerKey.Parallelism, 2);
    irDAGBuilder.addVertex(v5);

    final IREdge e1 = new IREdge(IREdge.Type.ScatterGather, v1, v2, Coder.DUMMY_CODER);
    e1.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
    e1.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);
    irDAGBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(IREdge.Type.ScatterGather, v3, v2, Coder.DUMMY_CODER);
    e2.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
    e2.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);
    irDAGBuilder.connectVertices(e2);

    final IREdge e4 = new IREdge(IREdge.Type.ScatterGather, v2, v4, Coder.DUMMY_CODER);
    e4.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
    e4.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);
    irDAGBuilder.connectVertices(e4);

    final IREdge e5 = new IREdge(IREdge.Type.ScatterGather, v2, v5, Coder.DUMMY_CODER);
    e5.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
    e5.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);
    irDAGBuilder.connectVertices(e5);

    final DAG<IRVertex, IREdge> irDAG = irDAGBuilder.buildWithoutSourceSinkCheck();
    final DAG<Stage, StageEdge> logicalDAG = irDAG.convert(new LogicalDAGGenerator());
    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = logicalDAG.convert(new PhysicalDAGGenerator());

    final JobStateManager jobStateManager =
        new JobStateManager(new PhysicalPlan("TestPlan", physicalDAG), new BlockManagerMaster());

    assertEquals(jobStateManager.getJobId(), "TestPlan");

    final List<PhysicalStage> stageList = physicalDAG.getTopologicalSort();
    for (int stageIdx = 0; stageIdx < stageList.size(); stageIdx++) {
      final PhysicalStage physicalStage = stageList.get(stageIdx);
      jobStateManager.onStageStateChanged(physicalStage.getId(), StageState.State.EXECUTING);
      final List<TaskGroup> taskGroupList = physicalStage.getTaskGroupList();
      taskGroupList.forEach(taskGroup -> {
        jobStateManager.onTaskGroupStateChanged(taskGroup.getTaskGroupId(),
            TaskGroupState.State.EXECUTING);
        jobStateManager.onTaskGroupStateChanged(taskGroup.getTaskGroupId(),
            TaskGroupState.State.COMPLETE);
        if (taskGroup.getTaskGroupIdx() == taskGroupList.size() - 1) {
          assertEquals(physicalStage.getId(), jobStateManager.checkStageCompletion(taskGroup.getTaskGroupId()).get());
        }
      });
      final Map<String, TaskGroupState> taskGroupStateMap = jobStateManager.getIdToTaskGroupStates();
      final Map<String, TaskState> taskStateMap = jobStateManager.getIdToTaskStates();
      taskGroupList.forEach(taskGroup -> {
        assertEquals(taskGroupStateMap.get(taskGroup.getTaskGroupId()).getStateMachine().getCurrentState(),
            TaskGroupState.State.COMPLETE);
        taskGroup.getTaskDAG().getVertices().forEach(
            task -> assertEquals(taskStateMap.get(task.getId()).getStateMachine().getCurrentState(),
                TaskState.State.COMPLETE));
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
  public void testWaitUntilFinish() throws Exception {
    // Create a JobStateManager of an empty dag.
    final DAG<IRVertex, IREdge> irDAG = irDAGBuilder.build();
    final DAG<Stage, StageEdge> logicalDAG = irDAG.convert(new LogicalDAGGenerator());
    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = logicalDAG.convert(new PhysicalDAGGenerator());
    final JobStateManager jobStateManager =
        new JobStateManager(new PhysicalPlan("TestPlan", physicalDAG), new BlockManagerMaster());

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
