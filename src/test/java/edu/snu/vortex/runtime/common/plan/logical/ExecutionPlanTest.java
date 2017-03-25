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
package edu.snu.vortex.runtime.common.plan.logical;

import edu.snu.vortex.compiler.ir.*;
import edu.snu.vortex.runtime.common.plan.logical.ExecutionPlan;
import edu.snu.vortex.runtime.common.plan.logical.ExecutionPlanBuilder;
import edu.snu.vortex.runtime.common.plan.logical.RuntimeStage;
import org.junit.Before;
import org.junit.Test;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link ExecutionPlanBuilder}
 */
public final class ExecutionPlanTest {
  private ExecutionPlanBuilder builder;

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
    builder.addVertex(v1);

    builder.createNewStage();
    final Vertex v2 = new OperatorVertex(t);
    builder.addVertex(v2);

    final DAGBuilder tempDAGBuilder = new DAGBuilder();
    tempDAGBuilder.addVertex(v1);
    tempDAGBuilder.addVertex(v2);
    final Edge e = tempDAGBuilder.connectVertices(v1, v2, Edge.Type.ScatterGather);
    builder.connectVertices(e);

    final ExecutionPlan plan = builder.build();
    final List<RuntimeStage> runtimeStages = plan.getRuntimeStages();

    assertEquals(runtimeStages.size(), 2);
    assertEquals(runtimeStages.get(0).getStageIncomingEdges().size(), 0);
    assertEquals(runtimeStages.get(1).getStageOutgoingEdges().size(), 0);
    assertEquals(runtimeStages.get(0).getStageOutgoingEdges().size(), 1);
    assertEquals(runtimeStages.get(1).getStageIncomingEdges().size(), 1);
  }

  @Test
  public void testPlan2() {
    // Tests a plan of 4 stages.
    final Transform t = mock(Transform.class);
    final Vertex v1 = new OperatorVertex(t);
    final Vertex v2 = new OperatorVertex(t);
    final Vertex v3 = new OperatorVertex(t);
    final Vertex v4 = new OperatorVertex(t);
    final Vertex v5 = new OperatorVertex(t);
    final Vertex v6 = new OperatorVertex(t);

    final DAGBuilder tempDAGBuilder = new DAGBuilder();
    tempDAGBuilder.addVertex(v1);
    tempDAGBuilder.addVertex(v2);
    tempDAGBuilder.addVertex(v3);
    tempDAGBuilder.addVertex(v4);
    tempDAGBuilder.addVertex(v5);
    tempDAGBuilder.addVertex(v6);

    final Edge e1 = tempDAGBuilder.connectVertices(v1, v2, Edge.Type.OneToOne);
    final Edge e2 = tempDAGBuilder.connectVertices(v1, v3, Edge.Type.OneToOne);
    final Edge e3 = tempDAGBuilder.connectVertices(v2, v4, Edge.Type.ScatterGather);
    final Edge e4 = tempDAGBuilder.connectVertices(v3, v5, Edge.Type.ScatterGather);
    final Edge e5 = tempDAGBuilder.connectVertices(v4, v6, Edge.Type.OneToOne);

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

    // Stage 3 = {v5}
    builder.createNewStage();
    builder.addVertex(v5);
    builder.connectVertices(e4);

    // Stage 4 = {v6}
    builder.createNewStage();
    builder.addVertex(v6);
    builder.connectVertices(e5);

    final ExecutionPlan plan = builder.build();
    final List<RuntimeStage> runtimeStages = plan.getRuntimeStages();

    assertEquals(runtimeStages.size(), 4);
    assertEquals(runtimeStages.get(0).getStageIncomingEdges().size(), 0);
    assertEquals(runtimeStages.get(0).getStageOutgoingEdges().size(), 2);

    assertEquals(runtimeStages.get(1).getStageIncomingEdges().size(), 1);
    assertEquals(runtimeStages.get(1).getStageOutgoingEdges().size(), 1);

    assertEquals(runtimeStages.get(2).getStageIncomingEdges().size(), 1);
    assertEquals(runtimeStages.get(2).getStageOutgoingEdges().size(), 0);

    assertEquals(runtimeStages.get(3).getStageIncomingEdges().size(), 1);
    assertEquals(runtimeStages.get(3).getStageOutgoingEdges().size(), 0);
  }
}
