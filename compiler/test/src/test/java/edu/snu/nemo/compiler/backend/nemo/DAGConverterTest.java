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
package edu.snu.nemo.compiler.backend.nemo;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DataFlowModelProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.InterTaskDataStoreProperty;
import edu.snu.nemo.common.ir.vertex.SourceVertex;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import edu.snu.nemo.compiler.frontend.beam.transform.DoTransform;
import edu.snu.nemo.compiler.optimizer.CompiletimeOptimizer;
import edu.snu.nemo.common.test.EmptyComponents;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.runtime.common.plan.PhysicalPlanGenerator;
import edu.snu.nemo.runtime.common.plan.Stage;
import edu.snu.nemo.runtime.common.plan.StageEdge;
import edu.snu.nemo.compiler.optimizer.policy.TestPolicy;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link PhysicalPlanGenerator}.
 */
public final class DAGConverterTest {
  private DAGBuilder<IRVertex, IREdge> irDAGBuilder;
  private PhysicalPlanGenerator physicalPlanGenerator;

  @Before
  public void setUp() throws Exception {
    irDAGBuilder = new DAGBuilder<>();
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(JobConf.DAGDirectory.class, "");
    physicalPlanGenerator = injector.getInstance(PhysicalPlanGenerator.class);
  }

  @Test
  public void testSimplePlan() throws Exception {
    final Transform t = mock(Transform.class);
    final IRVertex v1 = new OperatorVertex(t);
    v1.setProperty(ParallelismProperty.of(3));
    irDAGBuilder.addVertex(v1);

    final IRVertex v2 = new OperatorVertex(new DoTransform(null, null));
    v2.setProperty(ParallelismProperty.of(2));
    irDAGBuilder.addVertex(v2);

    final IREdge e = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, v1, v2);
    irDAGBuilder.connectVertices(e);

    final DAG<IRVertex, IREdge> irDAG = CompiletimeOptimizer.optimize(irDAGBuilder.buildWithoutSourceSinkCheck(),
        new TestPolicy(), "");
    final DAG<Stage, StageEdge> DAGOfStages = physicalPlanGenerator.stagePartitionIrDAG(irDAG);
    final DAG<Stage, StageEdge> physicalDAG = irDAG.convert(physicalPlanGenerator);

    // Test DAG of stages
    final List<Stage> sortedDAGOfStages = DAGOfStages.getTopologicalSort();
    final Stage stage1 = sortedDAGOfStages.get(0);
    final Stage stage2 = sortedDAGOfStages.get(1);

    assertEquals(DAGOfStages.getVertices().size(), 2);
    assertEquals(DAGOfStages.getIncomingEdgesOf(stage1).size(), 0);
    assertEquals(DAGOfStages.getIncomingEdgesOf(stage2).size(), 1);
    assertEquals(DAGOfStages.getOutgoingEdgesOf(stage1).size(), 1);
    assertEquals(DAGOfStages.getOutgoingEdgesOf(stage2).size(), 0);

    // Test Physical DAG
    final List<Stage> sortedPhysicalDAG = physicalDAG.getTopologicalSort();
    final Stage physicalStage1 = sortedPhysicalDAG.get(0);
    final Stage physicalStage2 = sortedPhysicalDAG.get(1);
    assertEquals(physicalDAG.getVertices().size(), 2);
    assertEquals(physicalDAG.getIncomingEdgesOf(physicalStage1).size(), 0);
    assertEquals(physicalDAG.getIncomingEdgesOf(physicalStage2).size(), 1);
    assertEquals(physicalDAG.getOutgoingEdgesOf(physicalStage1).size(), 1);
    assertEquals(physicalDAG.getOutgoingEdgesOf(physicalStage2).size(), 0);

    assertEquals(physicalStage1.getTaskIds().size(), 3);
    assertEquals(physicalStage2.getTaskIds().size(), 2);
  }

  @Test
  public void testComplexPlan() throws Exception {
    // Tests a plan of 4 stages.
    final SourceVertex s = new EmptyComponents.EmptySourceVertex("Source");

    final IRVertex v1 = s;
    v1.setProperty(ParallelismProperty.of(3));
    v1.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));

    final Transform t = mock(Transform.class);
    final DoTransform dt = new DoTransform(null, null);
    final IRVertex v2 = new OperatorVertex(t);
    v2.setProperty(ParallelismProperty.of(3));
    v2.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));

    final IRVertex v3 = new OperatorVertex(t);
    v3.setProperty(ParallelismProperty.of(3));
    v3.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));

    final IRVertex v4 = new OperatorVertex(t);
    v4.setProperty(ParallelismProperty.of(2));
    v4.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));

    final IRVertex v5 = new OperatorVertex(dt);
    v5.setProperty(ParallelismProperty.of(2));
    v5.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));

    final IRVertex v6 = new OperatorVertex(dt);
    v6.setProperty(ParallelismProperty.of(2));
    v6.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.RESERVED));

//    final IRVertex v7 = new OperatorVertex(t);
//    v7.setProperty(Parallelism.of(2));
//    v7.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));

    final IRVertex v8 = new OperatorVertex(dt);
    v8.setProperty(ParallelismProperty.of(2));
    v8.setProperty(ExecutorPlacementProperty.of(ExecutorPlacementProperty.COMPUTE));

    irDAGBuilder.addVertex(v1);
    irDAGBuilder.addVertex(v2);
    irDAGBuilder.addVertex(v3);
    irDAGBuilder.addVertex(v4);
    irDAGBuilder.addVertex(v5);
    irDAGBuilder.addVertex(v6);
    irDAGBuilder.addVertex(v8);

//    irDAGBuilder.addVertex(v7);

    final IREdge e1 = new IREdge(DataCommunicationPatternProperty.Value.OneToOne, v1, v2);
    e1.setProperty(InterTaskDataStoreProperty.of(InterTaskDataStoreProperty.Value.MemoryStore));
    e1.setProperty(DataFlowModelProperty.of(DataFlowModelProperty.Value.Pull));

    final IREdge e2 = new IREdge(DataCommunicationPatternProperty.Value.OneToOne, v1, v3);
    e2.setProperty(InterTaskDataStoreProperty.of(InterTaskDataStoreProperty.Value.MemoryStore));
    e2.setProperty(DataFlowModelProperty.of(DataFlowModelProperty.Value.Pull));

    final IREdge e3 = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, v2, v4);
    e3.setProperty(InterTaskDataStoreProperty.of(InterTaskDataStoreProperty.Value.MemoryStore));
    e3.setProperty(DataFlowModelProperty.of(DataFlowModelProperty.Value.Push));

    final IREdge e4 = new IREdge(DataCommunicationPatternProperty.Value.Shuffle, v3, v5);
    e4.setProperty(InterTaskDataStoreProperty.of(InterTaskDataStoreProperty.Value.MemoryStore));
    e4.setProperty(DataFlowModelProperty.of(DataFlowModelProperty.Value.Push));

    final IREdge e5 = new IREdge(DataCommunicationPatternProperty.Value.OneToOne, v4, v6);
    e5.setProperty(InterTaskDataStoreProperty.of(InterTaskDataStoreProperty.Value.LocalFileStore));
    e5.setProperty(DataFlowModelProperty.of(DataFlowModelProperty.Value.Pull));

    final IREdge e6 = new IREdge(DataCommunicationPatternProperty.Value.OneToOne, v4, v8);
    e6.setProperty(InterTaskDataStoreProperty.of(InterTaskDataStoreProperty.Value.LocalFileStore));
    e6.setProperty(DataFlowModelProperty.of(DataFlowModelProperty.Value.Pull));

//    final IREdge e7 = new IREdge(OneToOne, v7, v5);
//    e7.setProperty(InterTaskDataStoreProperty.of(MemoryStore));
//    e7.setProperty(Attribute.Key.PullOrPush, DataFlowModelProperty.Value.Push));
//
//    final IREdge e8 = new IREdge(OneToOne, v5, v8);
//    e8.setProperty(InterTaskDataStoreProperty.of(MemoryStore));
//    e8.setProperty(Attribute.Key.PullOrPush, DataFlowModelProperty.Value.Pull));

    // Stage 1 = {v1, v2, v3}
    irDAGBuilder.connectVertices(e1);
    irDAGBuilder.connectVertices(e2);

    // Stage 2 = {v4}
    irDAGBuilder.connectVertices(e3);

    // Stage 3 = {v7}
    // Commented out since SimpleRuntime does not yet support multi-input.
//    physicalDAGBuilder.createNewStage();
//    physicalDAGBuilder.addVertex(v7);

    // Stage 4 = {v5, v8}
    irDAGBuilder.connectVertices(e4);
    irDAGBuilder.connectVertices(e6);

    // Commented out since SimpleRuntime does not yet support multi-input.
//    irDAGBuilder.connectVertices(e7);
//    irDAGBuilder.connectVertices(e8);

    // Stage 5 = {v6}
    irDAGBuilder.connectVertices(e5);

    final DAG<IRVertex, IREdge> irDAG = CompiletimeOptimizer.optimize(irDAGBuilder.build(),
        new TestPolicy(), "");
    final DAG<Stage, StageEdge> logicalDAG = physicalPlanGenerator.stagePartitionIrDAG(irDAG);

    // Test Logical DAG
    final List<Stage> sortedLogicalDAG = logicalDAG.getTopologicalSort();
    final Stage stage1 = sortedLogicalDAG.get(0);
    final Stage stage2 = sortedLogicalDAG.get(1);
    final Stage stage3 = sortedLogicalDAG.get(2);
    final Stage stage4 = sortedLogicalDAG.get(3);
    final Stage stage5 = sortedLogicalDAG.get(3);

    // The following asserts depend on how stage partitioning is defined; test must be rewritten accordingly.
//    assertEquals(logicalDAG.getVertices().size(), 5);
//    assertEquals(logicalDAG.getIncomingEdgesOf(stage1).size(), 0);
//    assertEquals(logicalDAG.getIncomingEdgesOf(stage2).size(), 1);
//    assertEquals(logicalDAG.getIncomingEdgesOf(stage3).size(), 1);
//    assertEquals(logicalDAG.getIncomingEdgesOf(stage4).size(), 1);
//    assertEquals(logicalDAG.getOutgoingEdgesOf(stage1).size(), 2);
//    assertEquals(logicalDAG.getOutgoingEdgesOf(stage2).size(), 0);
//    assertEquals(logicalDAG.getOutgoingEdgesOf(stage3).size(), 1);
//    assertEquals(logicalDAG.getOutgoingEdgesOf(stage4).size(), 0);

    // Test Physical DAG

//    final DAG<Stage, StageEdge> physicalDAG = logicalDAG.convert(new PhysicalDAGGenerator());
//    final List<Stage> sortedPhysicalDAG = physicalDAG.getTopologicalSort();
//    final Stage physicalStage1 = sortedPhysicalDAG.get(0);
//    final Stage physicalStage2 = sortedPhysicalDAG.get(1);
//    assertEquals(physicalDAG.getVertices().size(), 2);
//    assertEquals(physicalDAG.getIncomingEdgesOf(physicalStage1).size(), 0);
//    assertEquals(physicalDAG.getIncomingEdgesOf(physicalStage2).size(), 1);
//    assertEquals(physicalDAG.getOutgoingEdgesOf(physicalStage1).size(), 1);
//    assertEquals(physicalDAG.getOutgoingEdgesOf(physicalStage2).size(), 0);
//
//    final List<Task> taskList1 = physicalStage1.getTaskList();
//    final List<Task> taskList2 = physicalStage2.getTaskList();
//    assertEquals(taskList1.size(), 3);
//    assertEquals(taskList2.size(), 2);
  }
}
