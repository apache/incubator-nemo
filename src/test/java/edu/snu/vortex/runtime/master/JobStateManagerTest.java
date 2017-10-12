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

import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.frontend.beam.transform.DoTransform;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.OperatorVertex;
import edu.snu.vortex.compiler.ir.Transform;
import edu.snu.vortex.compiler.ir.executionproperty.vertex.ParallelismProperty;
import edu.snu.vortex.compiler.optimizer.Optimizer;
import edu.snu.vortex.compiler.optimizer.TestPolicy;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.local.LocalMessageDispatcher;
import edu.snu.vortex.runtime.common.message.local.LocalMessageEnvironment;
import edu.snu.vortex.runtime.common.metric.MetricMessageHandler;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.runtime.common.state.JobState;
import edu.snu.vortex.runtime.common.state.StageState;
import edu.snu.vortex.runtime.common.state.TaskGroupState;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.common.dag.DAGBuilder;
import edu.snu.vortex.runtime.executor.datatransfer.communication.ScatterGather;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
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
  private PartitionManagerMaster partitionManagerMaster;
  private MetricMessageHandler metricMessageHandler;

  @Before
  public void setUp() throws InjectionException {
    irDAGBuilder = new DAGBuilder<>();
    final LocalMessageDispatcher messageDispatcher = new LocalMessageDispatcher();
    final LocalMessageEnvironment messageEnvironment =
        new LocalMessageEnvironment(MessageEnvironment.MASTER_COMMUNICATION_ID, messageDispatcher);
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(MessageEnvironment.class, messageEnvironment);
    partitionManagerMaster = injector.getInstance(PartitionManagerMaster.class);
    metricMessageHandler = mock(MetricMessageHandler.class);
  }

  /**
   * This method builds a physical DAG starting from an IR DAG and submits it to {@link JobStateManager}.
   * State changes are explicitly called to check whether states are managed correctly or not.
   */
  @Test
  public void testPhysicalPlanStateChanges() throws Exception {
    final Transform t = mock(Transform.class);
    final IRVertex v1 = new OperatorVertex(t);
    v1.setProperty(ParallelismProperty.of(3));
    irDAGBuilder.addVertex(v1);

    final IRVertex v2 = new OperatorVertex(t);
    v2.setProperty(ParallelismProperty.of(2));
    irDAGBuilder.addVertex(v2);

    final IRVertex v3 = new OperatorVertex(t);
    v3.setProperty(ParallelismProperty.of(3));
    irDAGBuilder.addVertex(v3);

    final IRVertex v4 = new OperatorVertex(t);
    v4.setProperty(ParallelismProperty.of(2));
    irDAGBuilder.addVertex(v4);

    final IRVertex v5 = new OperatorVertex(new DoTransform(null, null));
    v5.setProperty(ParallelismProperty.of(2));
    irDAGBuilder.addVertex(v5);

    final IREdge e1 = new IREdge(ScatterGather.class, v1, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e1);

    final IREdge e2 = new IREdge(ScatterGather.class, v3, v2, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e2);

    final IREdge e4 = new IREdge(ScatterGather.class, v2, v4, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e4);

    final IREdge e5 = new IREdge(ScatterGather.class, v2, v5, Coder.DUMMY_CODER);
    irDAGBuilder.connectVertices(e5);

    final DAG<IRVertex, IREdge> irDAG = Optimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(),
        new TestPolicy(), "");
    final PhysicalPlanGenerator physicalPlanGenerator =
        Tang.Factory.getTang().newInjector().getInstance(PhysicalPlanGenerator.class);
    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = irDAG.convert(physicalPlanGenerator);
    final JobStateManager jobStateManager = new JobStateManager(
        new PhysicalPlan("TestPlan", physicalDAG, physicalPlanGenerator.getTaskIRVertexMap()),
        partitionManagerMaster, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);

    assertEquals(jobStateManager.getJobId(), "TestPlan");

    final List<PhysicalStage> stageList = physicalDAG.getTopologicalSort();

    for (int stageIdx = 0; stageIdx < stageList.size(); stageIdx++) {
      final PhysicalStage physicalStage = stageList.get(stageIdx);
      jobStateManager.onStageStateChanged(physicalStage.getId(), StageState.State.EXECUTING);
      final List<TaskGroup> taskGroupList = physicalStage.getTaskGroupList();
      taskGroupList.forEach(taskGroup -> {
        jobStateManager.onTaskGroupStateChanged(taskGroup, TaskGroupState.State.EXECUTING);
        jobStateManager.onTaskGroupStateChanged(taskGroup, TaskGroupState.State.COMPLETE);
        if (taskGroup.getTaskGroupIdx() == taskGroupList.size() - 1) {
          assertTrue(jobStateManager.checkStageCompletion(taskGroup.getStageId()));
        }
      });
      final Map<String, TaskGroupState> taskGroupStateMap = jobStateManager.getIdToTaskGroupStates();
      taskGroupList.forEach(taskGroup -> {
        assertEquals(taskGroupStateMap.get(taskGroup.getTaskGroupId()).getStateMachine().getCurrentState(),
            TaskGroupState.State.COMPLETE);
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
    final PhysicalPlanGenerator physicalPlanGenerator =
        Tang.Factory.getTang().newInjector().getInstance(PhysicalPlanGenerator.class);
    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = irDAG.convert(physicalPlanGenerator);
    final JobStateManager jobStateManager = new JobStateManager(
        new PhysicalPlan("TestPlan", physicalDAG, physicalPlanGenerator.getTaskIRVertexMap()),
        partitionManagerMaster, metricMessageHandler, MAX_SCHEDULE_ATTEMPT);

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
