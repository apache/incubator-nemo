/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.vortex.runtime.common.plan.physical;

import edu.snu.vortex.compiler.ir.*;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.common.plan.logical.ExecutionPlan;
import edu.snu.vortex.runtime.common.plan.logical.ExecutionPlanBuilder;
import edu.snu.vortex.runtime.common.plan.logical.RuntimeStage;
import edu.snu.vortex.runtime.master.RuntimeMaster;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link ExecutionPlanBuilder}
 */
public final class PhysicalPlanTest {
  private ExecutionPlanBuilder builder;
  private final RuntimeMaster runtimeMaster = new RuntimeMaster();

  @Before
  public void setUp() {
    builder = new ExecutionPlanBuilder();
  }

  @Test
  public void testSimplePlan() {
    // Tests a simple plan of 2 operators.
    builder.createNewStage();

    final Transform t = mock(Transform.class);
    final Vertex v1 = new OperatorVertex(t);
    v1.setAttr(Attribute.IntegerKey.Parallelism, 3);
    builder.addVertex(v1);

    builder.createNewStage();
    final Vertex v2 = new OperatorVertex(t);
    v2.setAttr(Attribute.IntegerKey.Parallelism, 2);
    builder.addVertex(v2);

    final DAGBuilder tempDAGBuilder = new DAGBuilder();
    tempDAGBuilder.addVertex(v1);
    tempDAGBuilder.addVertex(v2);
    final Edge e = tempDAGBuilder.connectVertices(v1, v2, Edge.Type.ScatterGather);
    e.setAttr(Attribute.Key.EdgeChannel, Attribute.Memory);
    e.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);
    builder.connectVertices(e);

    final ExecutionPlan plan = builder.build();

    // Submit the created execution plan to Runtime Master.
    runtimeMaster.execute(plan);
  }

  @Test
  public void testComplexPlan() {
    // Tests a plan of 5 stages.
    final Transform t = mock(Transform.class);
    final Vertex v1 = new OperatorVertex(t);
    v1.setAttr(Attribute.IntegerKey.Parallelism, 3);
    v1.setAttr(Attribute.Key.Placement, Attribute.Compute);

    final Vertex v2 = new OperatorVertex(t);
    v2.setAttr(Attribute.IntegerKey.Parallelism, 3);
    v2.setAttr(Attribute.Key.Placement, Attribute.Compute);

    final Vertex v3 = new OperatorVertex(t);
    v3.setAttr(Attribute.IntegerKey.Parallelism, 3);
    v3.setAttr(Attribute.Key.Placement, Attribute.Compute);

    final Vertex v4 = new OperatorVertex(t);
    v4.setAttr(Attribute.IntegerKey.Parallelism, 2);
    v4.setAttr(Attribute.Key.Placement, Attribute.Compute);

    final Vertex v5 = new OperatorVertex(t);
    v5.setAttr(Attribute.IntegerKey.Parallelism, 2);
    v5.setAttr(Attribute.Key.Placement, Attribute.Compute);

    final Vertex v6 = new OperatorVertex(t);
    v6.setAttr(Attribute.IntegerKey.Parallelism, 2);
    v6.setAttr(Attribute.Key.Placement, Attribute.Reserved);

    final Vertex v7 = new OperatorVertex(t);
    v7.setAttr(Attribute.IntegerKey.Parallelism, 2);
    v7.setAttr(Attribute.Key.Placement, Attribute.Compute);

    final Vertex v8 = new OperatorVertex(t);
    v8.setAttr(Attribute.IntegerKey.Parallelism, 2);
    v8.setAttr(Attribute.Key.Placement, Attribute.Compute);

    final DAGBuilder tempDAGBuilder = new DAGBuilder();
    tempDAGBuilder.addVertex(v1);
    tempDAGBuilder.addVertex(v2);
    tempDAGBuilder.addVertex(v3);
    tempDAGBuilder.addVertex(v4);
    tempDAGBuilder.addVertex(v5);
    tempDAGBuilder.addVertex(v6);
    tempDAGBuilder.addVertex(v7);
    tempDAGBuilder.addVertex(v8);

    final Edge e1 = tempDAGBuilder.connectVertices(v1, v2, Edge.Type.OneToOne);
    e1.setAttr(Attribute.Key.EdgeChannel, Attribute.Memory);
    e1.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    final Edge e2 = tempDAGBuilder.connectVertices(v1, v3, Edge.Type.OneToOne);
    e2.setAttr(Attribute.Key.EdgeChannel, Attribute.Memory);
    e2.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    final Edge e3 = tempDAGBuilder.connectVertices(v2, v4, Edge.Type.ScatterGather);
    e3.setAttr(Attribute.Key.EdgeChannel, Attribute.TCPPipe);
    e3.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);

    final Edge e4 = tempDAGBuilder.connectVertices(v3, v5, Edge.Type.ScatterGather);
    e4.setAttr(Attribute.Key.EdgeChannel, Attribute.TCPPipe);
    e4.setAttr(Attribute.Key.CommunicationPattern, Attribute.ScatterGather);

    final Edge e5 = tempDAGBuilder.connectVertices(v4, v6, Edge.Type.OneToOne);
    e5.setAttr(Attribute.Key.EdgeChannel, Attribute.File);
    e5.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    final Edge e6 = tempDAGBuilder.connectVertices(v4, v8, Edge.Type.OneToOne);
    e6.setAttr(Attribute.Key.EdgeChannel, Attribute.File);
    e6.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    final Edge e7 = tempDAGBuilder.connectVertices(v7, v5, Edge.Type.OneToOne);
    e7.setAttr(Attribute.Key.EdgeChannel, Attribute.TCPPipe);
    e7.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    final Edge e8 = tempDAGBuilder.connectVertices(v5, v8, Edge.Type.OneToOne);
    e8.setAttr(Attribute.Key.EdgeChannel, Attribute.Memory);
    e8.setAttr(Attribute.Key.CommunicationPattern, Attribute.OneToOne);

    // Stage 1 = {v1, v2, v3}
    builder.createNewStage();
    builder.addVertex(v1);
    builder.addVertex(v2);
    builder.addVertex(v3);
    builder.connectVertices(e1);
    builder.connectVertices(e2);

    // Stage 2 = {v4}
    builder.createNewStage();
    builder.addVertex(v4);
    builder.connectVertices(e3);

    // Stage 3 = {v7}
    builder.createNewStage();
    builder.addVertex(v7);

    // Stage 4 = {v5, v8}
    builder.createNewStage();
    builder.addVertex(v5);
    builder.addVertex(v8);
    builder.connectVertices(e4);
    builder.connectVertices(e6);
    builder.connectVertices(e7);
    builder.connectVertices(e8);

    // Stage 5 = {v6}
    builder.createNewStage();
    builder.addVertex(v6);
    builder.connectVertices(e5);

    final ExecutionPlan plan = builder.build();

    // Submit the created execution plan to Runtime Master.
    runtimeMaster.execute(plan);
  }
}
