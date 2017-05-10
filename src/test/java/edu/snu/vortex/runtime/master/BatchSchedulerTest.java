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

import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.OperatorVertex;
import edu.snu.vortex.compiler.ir.Transform;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.plan.logical.LogicalDAGGenerator;
import edu.snu.vortex.runtime.common.plan.logical.Stage;
import edu.snu.vortex.runtime.common.plan.logical.StageEdge;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.runtime.common.state.StageState;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.runtime.common.state.TaskState;
import edu.snu.vortex.runtime.master.scheduler.BatchScheduler;
import edu.snu.vortex.utils.dag.DAG;
import edu.snu.vortex.utils.dag.DAGBuilder;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Tests {@link BatchScheduler}.
 * The tests in this class build a physical DAG starting from an IR DAG to test for a successful scheduling.
 * We know that the scheduling was successful if all tasks complete.
 */
public final class BatchSchedulerTest {
  private final BatchScheduler scheduler = new BatchScheduler(RuntimeAttribute.RoundRobin, 2000);
  private DAGBuilder<IRVertex, IREdge> irDAGBuilder;

  @Before
  public void setUp() {
    irDAGBuilder = new DAGBuilder<>();
  }

  /**
   * Tests a simple Map-Reduce DAG of 2 stages, one with parallelism 3 and the other with parallelism 2.
   */
  @Test
  public void testSimplePhysicalPlanScheduling() throws InterruptedException {
    final Transform t = mock(Transform.class);
    final IRVertex v1 = new OperatorVertex(t);
    v1.setAttr(Attribute.IntegerKey.Parallelism, 3);
    v1.setAttr(Attribute.Key.Placement, Attribute.Compute);
    irDAGBuilder.addVertex(v1);

    final IRVertex v2 = new OperatorVertex(t);
    v2.setAttr(Attribute.IntegerKey.Parallelism, 2);
    v2.setAttr(Attribute.Key.Placement, Attribute.Storage);
    irDAGBuilder.addVertex(v2);

    final IREdge e = new IREdge(IREdge.Type.ScatterGather, v1, v2);
    e.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
    e.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);
    irDAGBuilder.connectVertices(e);

    final DAG<IRVertex, IREdge> irDAG = irDAGBuilder.build();
    final DAG<Stage, StageEdge> logicalDAG = irDAG.convert(new LogicalDAGGenerator());
    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = logicalDAG.convert(new PhysicalDAGGenerator());

    // HACK: All executors have the same ID since we cannot deterministically know
    // which executor executes which task group,
    // but TaskGroupStateChangedMsg requires the source executor's ID.
    final ExecutorRepresenter a1 = new ExecutorRepresenter("a1", RuntimeAttribute.Compute, 1);
    final ExecutorRepresenter a2 = new ExecutorRepresenter("a1", RuntimeAttribute.Compute, 1);
    final ExecutorRepresenter a3 = new ExecutorRepresenter("a1", RuntimeAttribute.Compute, 1);
    final ExecutorRepresenter b1 = new ExecutorRepresenter("a1", RuntimeAttribute.Storage, 1);
    final ExecutorRepresenter b2 = new ExecutorRepresenter("a1", RuntimeAttribute.Storage, 1);

    scheduler.onExecutorAdded(a1);
    scheduler.onExecutorAdded(a2);
    scheduler.onExecutorAdded(a3);
    scheduler.onExecutorAdded(b1);
    scheduler.onExecutorAdded(b2);

    final ExecutionStateManager executionStateManager =
        scheduler.scheduleJob(new PhysicalPlan("TestPlan", physicalDAG));

    for (final PhysicalStage physicalStage : physicalDAG.getTopologicalSort()) {
      final StageState stageState =
          executionStateManager.getIdToStageStates().get(physicalStage.getId());
      while (stageState.getStateMachine().getCurrentState() == StageState.State.READY) {
        // Wait until this stage is scheduled and executing
      }
      physicalStage.getTaskGroupList().forEach(taskGroup -> {
        final TaskGroupState taskGroupState =
            executionStateManager.getIdToTaskGroupStates().get(taskGroup.getTaskGroupId());
        while (taskGroupState.getStateMachine().getCurrentState() == TaskGroupState.State.READY) {
          // Wait until this task group is scheduled and executing
        }
        final ControlMessage.TaskGroupStateChangedMsg.Builder taskGroupStateChangedMsg =
            ControlMessage.TaskGroupStateChangedMsg.newBuilder();
        taskGroupStateChangedMsg.setTaskGroupId(taskGroup.getTaskGroupId());
        taskGroupStateChangedMsg.setState(ControlMessage.TaskGroupStateFromExecutor.COMPLETE);
        scheduler.onTaskGroupStateChanged("a1", taskGroupStateChangedMsg.build());
      });
    }
    executionStateManager.getIdToTaskStates().forEach((id, state) ->
        assertEquals(state.getStateMachine().getCurrentState(), TaskState.State.COMPLETE));
    executionStateManager.getIdToTaskGroupStates().forEach((id, state) ->
        assertEquals(state.getStateMachine().getCurrentState(), TaskGroupState.State.COMPLETE));
    executionStateManager.getIdToStageStates().forEach((id, state) ->
        assertEquals(state.getStateMachine().getCurrentState(), StageState.State.COMPLETE));
    assertTrue(executionStateManager.checkJobCompletion());

    scheduler.terminate();
  }
}
