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

import com.google.common.collect.Sets;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.dag.Vertex;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import org.apache.nemo.common.ir.edge.executionproperty.MessageIdEdgeProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourcePriorityProperty;
import org.apache.nemo.common.ir.vertex.utility.StreamVertex;
import org.apache.nemo.common.test.EmptyComponents;
import org.apache.nemo.compiler.optimizer.NemoOptimizer;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultScheduleGroupPass;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.PlanRewriter;
import org.apache.nemo.runtime.common.plan.Stage;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test NemoPlanRewriter.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({NemoBackend.class, NemoOptimizer.class})
public final class NemoPlanRewriterTest {
  private static final Logger LOG = LoggerFactory.getLogger(NemoPlanRewriterTest.class.getName());

  private static final String DEBUG_DAG_DIRECTORY = "debug";
  private static final int MESSAGE_ID = 1;

  private NemoBackend backend;
  private IRDAG oldIR;
  private PhysicalPlan oldPlan;

  @Before
  public void setUp() throws Exception {
    // Backend
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(JobConf.DAGDirectory.class, DEBUG_DAG_DIRECTORY);
    this.backend = injector.getInstance(NemoBackend.class);

    // Old plan
    final DAGBuilder<IRVertex, IREdge> dagBuilder = new DAGBuilder<>();
    final IRVertex v1 = new EmptyComponents.EmptySourceVertex("empty source one");
    final IRVertex v2 = new OperatorVertex(new EmptyComponents.EmptyTransform("empty transform two"));
    final IRVertex v3 = new OperatorVertex(new EmptyComponents.EmptyTransform("empty transform three"));
    final IREdge e1 = new IREdge(CommunicationPatternProperty.Value.OneToOne, v1, v2);
    final IREdge e2 = new IREdge(CommunicationPatternProperty.Value.Shuffle, v2, v3);

    v1.setProperty(ParallelismProperty.of(2));
    v2.setProperty(ParallelismProperty.of(2));
    v3.setProperty(ParallelismProperty.of(3));
    e1.setProperty(MessageIdEdgeProperty.of(Sets.newHashSet(MESSAGE_ID)));
    e1.setProperty(DataFlowProperty.of(DataFlowProperty.Value.Push));
    e2.setProperty(DataFlowProperty.of(DataFlowProperty.Value.Pull));

    dagBuilder.addVertex(v1);
    dagBuilder.addVertex(v2);
    dagBuilder.addVertex(v3);
    dagBuilder.connectVertices(e1);
    dagBuilder.connectVertices(e2);

    this.oldIR = new IRDAG(dagBuilder.build());
    this.oldPlan = backend.compile(
      new DefaultScheduleGroupPass().apply(oldIR),
      element -> Optional.empty());
  }

  @Test
  public void testAnnotatingNonDataFlow() {
    final PlanRewriter planRewriter = getPlanRewriter(irdag -> {
      irdag.topologicalDo(v -> v.setProperty(ResourcePriorityProperty.of(ResourcePriorityProperty.TRANSIENT)));
      return irdag;
    });
    final PhysicalPlan newPlan = planRewriter.rewrite(oldPlan, MESSAGE_ID);
    assertEqualTopology(oldPlan, newPlan);
  }

  @Test
  public void testAnnotatingDataFlow() {
    final PlanRewriter planRewriter = getPlanRewriter(irdag -> {
      irdag.topologicalDo(v -> irdag
        .getIncomingEdgesOf(v)
        .forEach(e -> e.setProperty(DataFlowProperty.of(DataFlowProperty.Value.Push))));
      return irdag;
    });
    final PhysicalPlan newPlan = planRewriter.rewrite(oldPlan, MESSAGE_ID);
    assertEqualTopology(oldPlan, newPlan);
  }

  @Test
  public void testStreamVertex() {
    final PlanRewriter planRewriter = getPlanRewriter(irdag -> {
      irdag.topologicalDo(v -> irdag
        .getIncomingEdgesOf(v)
        .stream()
        .filter(e -> !CommunicationPatternProperty.Value.OneToOne
          .equals(e.getPropertyValue(CommunicationPatternProperty.class).get()))
        .forEach(e -> irdag.insert(new StreamVertex(), e)));
      return irdag;
    });

    // The ids of stages unrelated to the StreamVertex should remain the same
    final PhysicalPlan newPlan = planRewriter.rewrite(oldPlan, MESSAGE_ID);
    final Set<String> stagesIdsWithoutStreamVertex = newPlan.getStageDAG().getVertices().stream()
      .filter(this::isStageWithoutUtilityIRVertex)
      .map(Vertex::getId)
      .collect(Collectors.toSet());
    assertTrue(getOrderedStageIds(oldPlan).containsAll(stagesIdsWithoutStreamVertex));

    // The ids of stage edges unrelated to the StreamVertex should remain the same
    final Set<String> stageEdgeIdsWithoutStreamVertex = newPlan.getStageDAG().getEdges().stream()
      .filter(this::isStageEdgeWithoutUtilityIRVertex)
      .map(Edge::getId)
      .collect(Collectors.toSet());
    assertTrue(getOrderedStageEdgeIds(oldPlan).containsAll(stageEdgeIdsWithoutStreamVertex));
  }

  /*
  @Test
  public void testMessageBarrierVertex() {
    final PlanRewriter planRewriter = getPlanRewriter(irdag -> )
    // The ids must be exactly the same
    // except the newly inserted vertices
  }

  @Test
  public void testSamplingVertex() {
    final PlanRewriter planRewriter = getPlanRewriter(irdag -> )
    // The ids must be exactly the same
    // except the newly inserted vertices
  }
  */

  //////////////////////////////////////////// PRIVATE HELPER METHODS

  private boolean isStageWithoutUtilityIRVertex(final Stage stage) {
    return stage.getIRDAG().getVertices().stream().noneMatch(Util::isUtilityVertex);
  }

  private boolean isStageEdgeWithoutUtilityIRVertex(final StageEdge stageEdge) {
    return !Util.isUtilityVertex(stageEdge.getSrcIRVertex()) && !Util.isUtilityVertex(stageEdge.getDstIRVertex());
  }

  private PlanRewriter getPlanRewriter(final Function<IRDAG, IRDAG> irRewriteFunction) {
    final NemoOptimizer optimizer = mock(NemoOptimizer.class);
    when(optimizer.optimizeAtRunTime(any(), any())).thenAnswer(invocation -> {
      final IRDAG inputIRDAG = invocation.getArgument(0);
      final IRDAG outputIRDAG = new DefaultScheduleGroupPass().apply(irRewriteFunction.apply(inputIRDAG));
      return outputIRDAG;
    });
    final NemoPlanRewriter planRewriter = new NemoPlanRewriter(optimizer, backend);
    planRewriter.setIRDAG(oldIR);
    planRewriter.accumulate(MESSAGE_ID, new ArrayList<>());
    return planRewriter;
  }

  private List<String> getOrderedStageIds(final PhysicalPlan plan) {
    return plan.getStageDAG().getVertices().stream().map(Stage::getId).collect(Collectors.toList());
  }

  private List<String> getOrderedStageEdgeIds(final PhysicalPlan plan) {
    return plan.getStageDAG().getEdges().stream().map(StageEdge::getId).collect(Collectors.toList());
  }

  private List<List<String>> getOrderedMemberIRVertexIds(final PhysicalPlan plan) {
    return plan.getStageDAG().getVertices()
      .stream()
      .map(s -> s.getIRDAG().getVertices().stream().map(IRVertex::getId).collect(Collectors.toList()))
      .collect(Collectors.toList());
  }

  private List<List<String>> getOrderedMemberIREdgeIds(final PhysicalPlan plan) {
    return plan.getStageDAG().getVertices()
      .stream()
      .map(s -> s.getIRDAG().getEdges().stream().map(Edge::getId).collect(Collectors.toList()))
      .collect(Collectors.toList());
  }

  private void assertEqualTopology(final PhysicalPlan left, final PhysicalPlan right) {
    // Same Stage/StageEdge ids
    assertEquals(getOrderedStageIds(left), getOrderedStageIds(right));
    assertEquals(getOrderedStageEdgeIds(left), getOrderedStageEdgeIds(right));

    // Same member IRVertex/IREdge objects within each Stage
    assertEquals(getOrderedMemberIRVertexIds(left), getOrderedMemberIRVertexIds(right));
    assertEquals(getOrderedMemberIREdgeIds(left), getOrderedMemberIREdgeIds(right));
  }
}
