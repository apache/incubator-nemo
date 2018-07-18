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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.nemo.client.JobLauncher;
import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DataFlowModelProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ScheduleGroupProperty;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import edu.snu.nemo.common.test.EmptyComponents;
import edu.snu.nemo.compiler.CompilerTestUtil;
import edu.snu.nemo.compiler.optimizer.CompiletimeOptimizer;
import edu.snu.nemo.compiler.optimizer.policy.TestPolicy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static edu.snu.nemo.common.test.EmptyComponents.EMPTY_TRANSFORM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link DefaultScheduleGroupPass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public final class DefaultScheduleGroupPassTest {
  @Test
  public void testAnnotatingPass() {
    final AnnotatingPass scheduleGroupPass = new DefaultScheduleGroupPass();
    assertEquals(ScheduleGroupProperty.class, scheduleGroupPass.getExecutionPropertyToModify());
  }

  /**
   * This test ensures that a topologically sorted DAG has an increasing sequence of schedule group indexes.
   */
  @Test
  public void testTopologicalOrdering() throws Exception {
    final DAG<IRVertex, IREdge> compiledDAG = CompilerTestUtil.compileALSDAG();
    final DAG<IRVertex, IREdge> processedDAG = CompiletimeOptimizer.optimize(compiledDAG,
        new TestPolicy(), "");

    for (final IRVertex irVertex : processedDAG.getTopologicalSort()) {
      final Integer currentScheduleGroup = irVertex.getPropertyValue(ScheduleGroupProperty.class).get();
      final Integer largestScheduleGroupOfParent = processedDAG.getParents(irVertex.getId()).stream()
          .mapToInt(v -> v.getPropertyValue(ScheduleGroupProperty.class).get())
          .max().orElse(0);
      assertTrue(currentScheduleGroup >= largestScheduleGroupOfParent);
    }
  }

  /**
   * Return a DAG that has a branch.
   * {@literal
   *           /-- v3 --- v4
   * v0 --- v1 --- v2 --/
   * }
   *
   * @param communicationPattern {@link DataCommunicationPatternProperty.Value} for the edges
   * @param dataFlowModel {@link DataFlowModelProperty.Value} for the edges
   * @return a {@link Pair} of {@link DAG} and {@link List} of {@link IRVertex}
   */
  private static Pair<DAG<IRVertex, IREdge>, List<IRVertex>> generateBranchDAG(
      final DataCommunicationPatternProperty.Value communicationPattern,
      final DataFlowModelProperty.Value dataFlowModel) {
    final DAGBuilder<IRVertex, IREdge> dagBuilder = new DAGBuilder<>();

    final IRVertex v0 = new OperatorVertex(EMPTY_TRANSFORM);
    final IRVertex v1 = new OperatorVertex(EMPTY_TRANSFORM);
    final IRVertex v2 = new OperatorVertex(EMPTY_TRANSFORM);
    final IRVertex v3 = new OperatorVertex(EMPTY_TRANSFORM);
    final IRVertex v4 = new OperatorVertex(EMPTY_TRANSFORM);

    final IREdge e0 = new IREdge(communicationPattern, v0, v1);
    final IREdge e1 = new IREdge(communicationPattern, v1, v2);
    final IREdge e2 = new IREdge(communicationPattern, v1, v3);
    final IREdge e3 = new IREdge(communicationPattern, v2, v4);
    final IREdge e4 = new IREdge(communicationPattern, v3, v4);

    final List<IRVertex> vertices = Arrays.asList(v0, v1, v2, v3, v4);
    for (final IRVertex vertex : vertices) {
      dagBuilder.addVertex(vertex);
    }
    for (final IREdge edge : Arrays.asList(e0, e1, e2, e3, e4)) {
      edge.getExecutionProperties().put(DataFlowModelProperty.of(dataFlowModel));
      dagBuilder.connectVertices(edge);
    }
    return Pair.of(dagBuilder.buildWithoutSourceSinkCheck(), vertices);
  }

  /**
   * Return a DAG that has a join.
   * {@literal
   * v0 --- v1 --- v4 -- v5
   * v2 --- v3 --/
   * }
   *
   * @param communicationPattern {@link DataCommunicationPatternProperty.Value} for the edges
   * @param dataFlowModel {@link DataFlowModelProperty.Value} for the edges
   * @return a {@link Pair} of {@link DAG} and {@link List} of {@link IRVertex}
   */
  private static Pair<DAG<IRVertex, IREdge>, List<IRVertex>> generateJoinDAG(
      final DataCommunicationPatternProperty.Value communicationPattern,
      final DataFlowModelProperty.Value dataFlowModel) {
    final DAGBuilder<IRVertex, IREdge> dagBuilder = new DAGBuilder<>();

    final IRVertex v0 = new OperatorVertex(EMPTY_TRANSFORM);
    final IRVertex v1 = new OperatorVertex(EMPTY_TRANSFORM);
    final IRVertex v2 = new OperatorVertex(EMPTY_TRANSFORM);
    final IRVertex v3 = new OperatorVertex(EMPTY_TRANSFORM);
    final IRVertex v4 = new OperatorVertex(EMPTY_TRANSFORM);
    final IRVertex v5 = new OperatorVertex(EMPTY_TRANSFORM);

    final IREdge e0 = new IREdge(communicationPattern, v0, v1);
    final IREdge e1 = new IREdge(communicationPattern, v2, v3);
    final IREdge e2 = new IREdge(communicationPattern, v1, v4);
    final IREdge e3 = new IREdge(communicationPattern, v3, v4);
    final IREdge e4 = new IREdge(communicationPattern, v4, v5);

    final List<IRVertex> vertices = Arrays.asList(v0, v1, v2, v3, v4, v5);
    for (final IRVertex vertex : vertices) {
      dagBuilder.addVertex(vertex);
    }
    for (final IREdge edge : Arrays.asList(e0, e1, e2, e3, e4)) {
      edge.getExecutionProperties().put(DataFlowModelProperty.of(dataFlowModel));
      dagBuilder.connectVertices(edge);
    }
    return Pair.of(dagBuilder.buildWithoutSourceSinkCheck(), vertices);
  }

  /**
   * Asserts that the {@link ScheduleGroupProperty} is equal to {@code expected}.
   * @param expected the expected property value
   * @param vertex the vertex to test
   */
  private static void assertScheduleGroup(final int expected, final IRVertex vertex) {
    assertEquals(expected, getScheduleGroup(vertex));
  }

  /**
   * @param vertex a vertex
   * @return {@link ScheduleGroupProperty} of {@code vertex}
   */
  private static int getScheduleGroup(final IRVertex vertex) {
    return vertex.getPropertyValue(ScheduleGroupProperty.class)
        .orElseThrow(() -> new RuntimeException(String.format("ScheduleGroup not set for %s", vertex.getId())));
  }

  /**
   * Ensures that all vertices in {@code vertices} have different {@link ScheduleGroupProperty} value.
   * @param vertices vertices to test
   */
  private static void assertDifferentScheduleGroup(final Collection<IRVertex> vertices) {
    final Set<Integer> indices = new HashSet<>();
    vertices.forEach(v -> {
      final int idx = getScheduleGroup(v);
      assertFalse(indices.contains(idx));
      indices.add(idx);
    });
  }

  /**
   * Test scenario when {@code allowMultipleInEdgesWithinScheduleGroup} is {@code true} and the DAG contains a branch.
   */
  @Test
  public void testBranch() {
    final DefaultScheduleGroupPass pass = new DefaultScheduleGroupPass();
    final Pair<DAG<IRVertex, IREdge>, List<IRVertex>> dag
        = generateBranchDAG(DataCommunicationPatternProperty.Value.OneToOne, DataFlowModelProperty.Value.Pull);
    pass.apply(dag.left());
    dag.right().forEach(v -> assertScheduleGroup(0, v));
  }

  /**
   * Test scenario when {@code allowMultipleInEdgesWithinScheduleGroup} is {@code false} and the DAG contains a branch.
   */
  @Test
  public void testBranchWhenMultipleInEdgeNotAllowed() {
    final DefaultScheduleGroupPass pass = new DefaultScheduleGroupPass(false, false, false);
    final Pair<DAG<IRVertex, IREdge>, List<IRVertex>> dag
        = generateBranchDAG(DataCommunicationPatternProperty.Value.OneToOne, DataFlowModelProperty.Value.Pull);
    pass.apply(dag.left());
    dag.right().subList(0, 4).forEach(v -> assertScheduleGroup(0, v));
    dag.right().subList(4, 5).forEach(v -> assertScheduleGroup(1, v));
  }

  /**
   * Test scenario to determine whether push edges properly enforces same scheduleGroup or not.
   */
  @Test
  public void testBranchWithPush() {
    final DefaultScheduleGroupPass pass = new DefaultScheduleGroupPass(false, false, false);
    final Pair<DAG<IRVertex, IREdge>, List<IRVertex>> dag
        = generateBranchDAG(DataCommunicationPatternProperty.Value.Shuffle, DataFlowModelProperty.Value.Push);
    pass.apply(dag.left());
    dag.right().forEach(v -> assertScheduleGroup(0, v));
  }

  /**
   * Test scenario when {@code allowBroadcastWithinScheduleGroup} is {@code false} and DAG contains Broadcast edges.
   */
  @Test
  public void testBranchWithBroadcast() {
    final DefaultScheduleGroupPass pass = new DefaultScheduleGroupPass(false, true, true);
    final Pair<DAG<IRVertex, IREdge>, List<IRVertex>> dag
        = generateBranchDAG(DataCommunicationPatternProperty.Value.BroadCast, DataFlowModelProperty.Value.Pull);
    assertDifferentScheduleGroup(pass.apply(dag.left()).getVertices());
  }

  /**
   * Test scenario when {@code allowShuffleWithinScheduleGroup} is {@code false} and DAG contains Shuffle edges.
   */
  @Test
  public void testBranchWithShuffle() {
    final DefaultScheduleGroupPass pass = new DefaultScheduleGroupPass(true, false, true);
    final Pair<DAG<IRVertex, IREdge>, List<IRVertex>> dag
        = generateBranchDAG(DataCommunicationPatternProperty.Value.Shuffle, DataFlowModelProperty.Value.Pull);
    assertDifferentScheduleGroup(pass.apply(dag.left()).getVertices());
  }

  /**
   * Test scenario when {@code allowMultipleInEdgesWithinScheduleGroup} is {@code true} and the DAG contains a join.
   */
  @Test
  public void testJoin() {
    final DefaultScheduleGroupPass pass = new DefaultScheduleGroupPass();
    final Pair<DAG<IRVertex, IREdge>, List<IRVertex>> dag
        = generateJoinDAG(DataCommunicationPatternProperty.Value.OneToOne, DataFlowModelProperty.Value.Pull);
    pass.apply(dag.left());
    final int idxForFirstScheduleGroup = getScheduleGroup(dag.right().get(0));
    final int idxForSecondScheduleGroup = getScheduleGroup(dag.right().get(2));
    dag.right().subList(0, 2).forEach(v -> assertScheduleGroup(idxForFirstScheduleGroup, v));
    dag.right().subList(2, 4).forEach(v -> assertScheduleGroup(idxForSecondScheduleGroup, v));
    dag.right().subList(4, 6).forEach(v -> assertScheduleGroup(2, v));
  }

  /**
   * Test scenario with multiple push inEdges.
   */
  @Test
  public void testJoinWithPush() {
    final DefaultScheduleGroupPass pass = new DefaultScheduleGroupPass();
    final Pair<DAG<IRVertex, IREdge>, List<IRVertex>> dag
        = generateJoinDAG(DataCommunicationPatternProperty.Value.OneToOne, DataFlowModelProperty.Value.Push);
    pass.apply(dag.left());
    dag.right().forEach(v -> assertScheduleGroup(0, v));
  }

  /**
   * Test scenario when single push inEdges.
   */
  @Test
  public void testJoinWithSinglePush() {
    final DefaultScheduleGroupPass pass = new DefaultScheduleGroupPass();
    final Pair<DAG<IRVertex, IREdge>, List<IRVertex>> dag
        = generateJoinDAG(DataCommunicationPatternProperty.Value.OneToOne, DataFlowModelProperty.Value.Push);
    dag.left().getOutgoingEdgesOf(dag.right().get(1)).iterator().next()
        .getExecutionProperties().put(DataFlowModelProperty.of(DataFlowModelProperty.Value.Pull));
    pass.apply(dag.left());
    final int idxForFirstScheduleGroup = getScheduleGroup(dag.right().get(0));
    final int idxForSecondScheduleGroup = getScheduleGroup(dag.right().get(2));
    dag.right().subList(0, 2).forEach(v -> assertScheduleGroup(idxForFirstScheduleGroup, v));
    dag.right().subList(2, 6).forEach(v -> assertScheduleGroup(idxForSecondScheduleGroup, v));
  }
}
