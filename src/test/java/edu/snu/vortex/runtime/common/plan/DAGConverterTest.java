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
package edu.snu.vortex.runtime.common.plan;

import edu.snu.vortex.compiler.frontend.beam.BoundedSourceVertex;
import edu.snu.vortex.compiler.ir.*;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.common.plan.logical.LogicalDAGGenerator;
import edu.snu.vortex.runtime.common.plan.logical.Stage;
import edu.snu.vortex.runtime.common.plan.logical.StageEdge;
import edu.snu.vortex.runtime.common.plan.physical.*;
import edu.snu.vortex.utils.dag.DAG;
import edu.snu.vortex.utils.dag.DAGBuilder;
import org.apache.beam.sdk.io.BoundedSource;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link LogicalDAGGenerator} and {@link PhysicalDAGGenerator}
 */
public final class DAGConverterTest {
  private DAGBuilder<IRVertex, IREdge> irDAGBuilder;

  @Before
  public void setUp() {
    irDAGBuilder = new DAGBuilder<>();
  }

  @Test
  public void testSimplePlan() {
    final Transform t = mock(Transform.class);
    final IRVertex v1 = new OperatorVertex(t);
    v1.setAttr(Attribute.IntegerKey.Parallelism, 3);
    irDAGBuilder.addVertex(v1);

    final IRVertex v2 = new OperatorVertex(t);
    v2.setAttr(Attribute.IntegerKey.Parallelism, 2);
    irDAGBuilder.addVertex(v2);

    final IREdge e = new IREdge(IREdge.Type.ScatterGather, v1, v2);
    e.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
    e.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);
    irDAGBuilder.connectVertices(e);

    final DAG<IRVertex, IREdge> irDAG = irDAGBuilder.build();
    final DAG<Stage, StageEdge> logicalDAG = irDAG.convert(new LogicalDAGGenerator());
    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = logicalDAG.convert(new PhysicalDAGGenerator());

    // Test Logical DAG
    final List<Stage> sortedLogicalDAG = logicalDAG.getTopologicalSort();
    final Stage stage1 = sortedLogicalDAG.get(0);
    final Stage stage2 = sortedLogicalDAG.get(1);

    assertEquals(logicalDAG.getVertices().size(), 2);
    assertEquals(logicalDAG.getIncomingEdgesOf(stage1).size(), 0);
    assertEquals(logicalDAG.getIncomingEdgesOf(stage2).size(), 1);
    assertEquals(logicalDAG.getOutgoingEdgesOf(stage1).size(), 1);
    assertEquals(logicalDAG.getOutgoingEdgesOf(stage2).size(), 0);

    // Test Physical DAG
    final List<PhysicalStage> sortedPhysicalDAG = physicalDAG.getTopologicalSort();
    final PhysicalStage physicalStage1 = sortedPhysicalDAG.get(0);
    final PhysicalStage physicalStage2 = sortedPhysicalDAG.get(1);
    assertEquals(physicalDAG.getVertices().size(), 2);
    assertEquals(physicalDAG.getIncomingEdgesOf(physicalStage1).size(), 0);
    assertEquals(physicalDAG.getIncomingEdgesOf(physicalStage2).size(), 1);
    assertEquals(physicalDAG.getOutgoingEdgesOf(physicalStage1).size(), 1);
    assertEquals(physicalDAG.getOutgoingEdgesOf(physicalStage2).size(), 0);

    final List<TaskGroup> taskGroupList1 = physicalStage1.getTaskGroupList();
    final List<TaskGroup> taskGroupList2 = physicalStage2.getTaskGroupList();
    assertEquals(taskGroupList1.size(), 3);
    assertEquals(taskGroupList2.size(), 2);
  }

  @Test
  public void testComplexPlan() throws Exception {
    // Tests a plan of 4 stages.
    final BoundedSource s = mock(BoundedSource.class);
    final BoundedSource.BoundedReader r = mock(BoundedSource.BoundedReader.class);
    final List<BoundedSource.BoundedReader> dummyReaderList = new ArrayList<>(3);
    dummyReaderList.add(r);
    dummyReaderList.add(r);
    dummyReaderList.add(r);
    when(s.getEstimatedSizeBytes(null)).thenReturn(9L);
    when(s.createReader(null)).thenReturn(r);
    when(s.splitIntoBundles(3L, null)).thenReturn(dummyReaderList);

    final IRVertex v1 = new BoundedSourceVertex<>(s);
    v1.setAttr(Attribute.IntegerKey.Parallelism, 3);
    v1.setAttr(Attribute.Key.Placement, Attribute.Compute);

    final Transform t = mock(Transform.class);
    final IRVertex v2 = new OperatorVertex(t);
    v2.setAttr(Attribute.IntegerKey.Parallelism, 3);
    v2.setAttr(Attribute.Key.Placement, Attribute.Compute);

    final IRVertex v3 = new OperatorVertex(t);
    v3.setAttr(Attribute.IntegerKey.Parallelism, 3);
    v3.setAttr(Attribute.Key.Placement, Attribute.Compute);

    final IRVertex v4 = new OperatorVertex(t);
    v4.setAttr(Attribute.IntegerKey.Parallelism, 2);
    v4.setAttr(Attribute.Key.Placement, Attribute.Compute);

    final IRVertex v5 = new OperatorVertex(t);
    v5.setAttr(Attribute.IntegerKey.Parallelism, 2);
    v5.setAttr(Attribute.Key.Placement, Attribute.Compute);

    final IRVertex v6 = new OperatorVertex(t);
    v6.setAttr(Attribute.IntegerKey.Parallelism, 2);
    v6.setAttr(Attribute.Key.Placement, Attribute.Reserved);

    // TODO #13: Implement Join Node
//    final IRVertex v7 = new OperatorVertex(t);
//    v7.setAttr(Attribute.IntegerKey.Parallelism, 2);
//    v7.setAttr(Attribute.Key.Placement, Attribute.Compute);

    final IRVertex v8 = new OperatorVertex(t);
    v8.setAttr(Attribute.IntegerKey.Parallelism, 2);
    v8.setAttr(Attribute.Key.Placement, Attribute.Compute);

    irDAGBuilder.addVertex(v1);
    irDAGBuilder.addVertex(v2);
    irDAGBuilder.addVertex(v3);
    irDAGBuilder.addVertex(v4);
    irDAGBuilder.addVertex(v5);
    irDAGBuilder.addVertex(v6);
    irDAGBuilder.addVertex(v8);

    // TODO #13: Implement Join Node
//    irDAGBuilder.addVertex(v7);

    final IREdge e1 = new IREdge(IREdge.Type.OneToOne, v1, v2);
    e1.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Local);
    e1.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Pull);
    e1.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    final IREdge e2 = new IREdge(IREdge.Type.OneToOne, v1, v3);
    e2.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Local);
    e2.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Pull);
    e2.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    final IREdge e3 = new IREdge(IREdge.Type.ScatterGather, v2, v4);
    e3.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
    e3.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Push);
    e3.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);

    final IREdge e4 = new IREdge(IREdge.Type.ScatterGather, v3, v5);
    e4.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.Memory);
    e4.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Push);
    e4.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);

    final IREdge e5 = new IREdge(IREdge.Type.OneToOne, v4, v6);
    e5.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.File);
    e5.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Pull);
    e5.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    final IREdge e6 = new IREdge(IREdge.Type.OneToOne, v4, v8);
    e6.setAttr(Attribute.Key.ChannelDataPlacement, Attribute.File);
    e6.setAttr(Attribute.Key.ChannelTransferPolicy, Attribute.Pull);
    e6.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    // TODO #13: Implement Join Node
//    final IREdge e7 = new IREdge(IREdge.Type.OneToOne, v7, v5);
//    e7.setAttr(Attribute.Key.BlockStore, Attribute.Memory);
//    e7.setAttr(Attribute.Key.PullOrPush, Attribute.Push);
//    e7.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);
//
//    final IREdge e8 = new IREdge(IREdge.Type.OneToOne, v5, v8);
//    e8.setAttr(Attribute.Key.BlockStore, Attribute.Local);
//    e8.setAttr(Attribute.Key.PullOrPush, Attribute.Pull);
//    e8.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    // Stage 1 = {v1, v2, v3}
    irDAGBuilder.connectVertices(e1);
    irDAGBuilder.connectVertices(e2);

    // Stage 2 = {v4}
    irDAGBuilder.connectVertices(e3);

    // Stage 3 = {v7}
    // Commented out since SimpleRuntime does not yet support multi-input.
    // TODO #13: Implement Join Node
//    physicalDAGBuilder.createNewStage();
//    physicalDAGBuilder.addVertex(v7);

    // Stage 4 = {v5, v8}
    irDAGBuilder.connectVertices(e4);
    irDAGBuilder.connectVertices(e6);

    // Commented out since SimpleRuntime does not yet support multi-input.
    // TODO #13: Implement Join Node
//    irDAGBuilder.connectVertices(e7);
//    irDAGBuilder.connectVertices(e8);

    // Stage 5 = {v6}
    irDAGBuilder.connectVertices(e5);

    final DAG<IRVertex, IREdge> irDAG = irDAGBuilder.build();
    final DAG<Stage, StageEdge> logicalDAG = irDAG.convert(new LogicalDAGGenerator());

    // Test Logical DAG
    final List<Stage> sortedLogicalDAG = logicalDAG.getTopologicalSort();
    final Stage stage1 = sortedLogicalDAG.get(0);
    final Stage stage2 = sortedLogicalDAG.get(1);
    final Stage stage3 = sortedLogicalDAG.get(2);
    final Stage stage4 = sortedLogicalDAG.get(3);
    final Stage stage5 = sortedLogicalDAG.get(3);

    // TODO #148: Optimize Stage Partitioning Algorithm in VortexBackend
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

//    final DAG<PhysicalStage, PhysicalStageEdge> physicalDAG = logicalDAG.convert(new PhysicalDAGGenerator());
//    final List<PhysicalStage> sortedPhysicalDAG = physicalDAG.getTopologicalSort();
//    final PhysicalStage physicalStage1 = sortedPhysicalDAG.get(0);
//    final PhysicalStage physicalStage2 = sortedPhysicalDAG.get(1);
//    assertEquals(physicalDAG.getVertices().size(), 2);
//    assertEquals(physicalDAG.getIncomingEdgesOf(physicalStage1).size(), 0);
//    assertEquals(physicalDAG.getIncomingEdgesOf(physicalStage2).size(), 1);
//    assertEquals(physicalDAG.getOutgoingEdgesOf(physicalStage1).size(), 1);
//    assertEquals(physicalDAG.getOutgoingEdgesOf(physicalStage2).size(), 0);
//
//    final List<TaskGroup> taskGroupList1 = physicalStage1.getTaskGroupList();
//    final List<TaskGroup> taskGroupList2 = physicalStage2.getTaskGroupList();
//    assertEquals(taskGroupList1.size(), 3);
//    assertEquals(taskGroupList2.size(), 2);
  }
}
