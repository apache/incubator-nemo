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
package org.apache.nemo.compiler.backend.nemo;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.common.test.EmptyComponents;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.plan.PhysicalPlanGenerator;
import org.apache.nemo.runtime.common.plan.Stage;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.compiler.optimizer.policy.TestPolicy;
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

    final IRVertex v2 = new OperatorVertex(mock(Transform.class));
    v2.setProperty(ParallelismProperty.of(2));
    irDAGBuilder.addVertex(v2);

    final IREdge e = new IREdge(CommunicationPatternProperty.Value.Shuffle, v1, v2);
    irDAGBuilder.connectVertices(e);

    final IRDAG irDAG = new TestPolicy().runCompileTimeOptimization(
        new IRDAG(irDAGBuilder.buildWithoutSourceSinkCheck()), DAG.EMPTY_DAG_DIRECTORY);
    final DAG<Stage, StageEdge> DAGOfStages = physicalPlanGenerator.stagePartitionIrDAG(irDAG);
    final DAG<Stage, StageEdge> physicalDAG = physicalPlanGenerator.apply(irDAG);

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

    assertEquals(3, physicalStage1.getTaskIndices().size());
    assertEquals(2, physicalStage2.getTaskIndices().size());
  }

  @Test
  public void testComplexPlan() throws Exception {
    // Tests a plan of 4 stages.
    final SourceVertex s = new EmptyComponents.EmptySourceVertex("Source");

    final IRVertex v1 = s;
    v1.setProperty(ParallelismProperty.of(3));
    v1.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.COMPUTE));

    final Transform t = mock(Transform.class);
    final Transform dt = mock(Transform.class);
    final IRVertex v2 = new OperatorVertex(t);
    v2.setProperty(ParallelismProperty.of(3));
    v2.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.COMPUTE));

    final IRVertex v3 = new OperatorVertex(t);
    v3.setProperty(ParallelismProperty.of(3));
    v3.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.COMPUTE));

    final IRVertex v4 = new OperatorVertex(t);
    v4.setProperty(ParallelismProperty.of(2));
    v4.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.COMPUTE));

    final IRVertex v5 = new OperatorVertex(dt);
    v5.setProperty(ParallelismProperty.of(2));
    v5.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.COMPUTE));

    final IRVertex v6 = new OperatorVertex(dt);
    v6.setProperty(ParallelismProperty.of(2));
    v6.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.RESERVED));

    final IRVertex v8 = new OperatorVertex(dt);
    v8.setProperty(ParallelismProperty.of(2));
    v8.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.COMPUTE));

    irDAGBuilder.addVertex(v1);
    irDAGBuilder.addVertex(v2);
    irDAGBuilder.addVertex(v3);
    irDAGBuilder.addVertex(v4);
    irDAGBuilder.addVertex(v5);
    irDAGBuilder.addVertex(v6);
    irDAGBuilder.addVertex(v8);

    final IREdge e1 = new IREdge(CommunicationPatternProperty.Value.OneToOne, v1, v2);
    e1.setProperty(DataStoreProperty.of(DataStoreProperty.Value.MemoryStore));
    e1.setProperty(DataFlowProperty.of(DataFlowProperty.Value.Pull));

    final IREdge e2 = new IREdge(CommunicationPatternProperty.Value.OneToOne, v1, v3);
    e2.setProperty(DataStoreProperty.of(DataStoreProperty.Value.MemoryStore));
    e2.setProperty(DataFlowProperty.of(DataFlowProperty.Value.Pull));

    final IREdge e3 = new IREdge(CommunicationPatternProperty.Value.Shuffle, v2, v4);
    e3.setProperty(DataStoreProperty.of(DataStoreProperty.Value.MemoryStore));
    e3.setProperty(DataFlowProperty.of(DataFlowProperty.Value.Push));

    final IREdge e4 = new IREdge(CommunicationPatternProperty.Value.Shuffle, v3, v5);
    e4.setProperty(DataStoreProperty.of(DataStoreProperty.Value.MemoryStore));
    e4.setProperty(DataFlowProperty.of(DataFlowProperty.Value.Push));

    final IREdge e5 = new IREdge(CommunicationPatternProperty.Value.OneToOne, v4, v6);
    e5.setProperty(DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore));
    e5.setProperty(DataFlowProperty.of(DataFlowProperty.Value.Pull));

    final IREdge e6 = new IREdge(CommunicationPatternProperty.Value.OneToOne, v4, v8);
    e6.setProperty(DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore));
    e6.setProperty(DataFlowProperty.of(DataFlowProperty.Value.Pull));
  }
}
