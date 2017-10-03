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
package edu.snu.vortex.compiler.optimizer.pass.compiletime.reshaping;

import edu.snu.vortex.client.JobLauncher;
import edu.snu.vortex.compiler.CompilerTestUtil;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.LoopVertex;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.common.dag.DAGBuilder;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link LoopOptimizations.LoopFusionPass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class LoopFusionPassTest {
  private DAG<IRVertex, IREdge> originalALSDAG;
  private DAG<IRVertex, IREdge> groupedDAG;
  private DAG<IRVertex, IREdge> dagToBeFused;
  private DAG<IRVertex, IREdge> dagNotToBeFused;
  private DAG<IRVertex, IREdge> dagToBePartiallyFused;
  private DAG<IRVertex, IREdge> dagToBePartiallyFused2;
  private DAG<IRVertex, IREdge> dagToBePartiallyFused3;

  @Before
  public void setUp() throws Exception {
    final DAGBuilder<IRVertex, IREdge> dagToBeFusedBuilder = new DAGBuilder<>();
    final DAGBuilder<IRVertex, IREdge> dagNotToBeFusedBuilder = new DAGBuilder<>();

    originalALSDAG = CompilerTestUtil.compileALSDAG();
    groupedDAG = new LoopGroupingPass().apply(originalALSDAG);

    groupedDAG.topologicalDo(v -> {
      dagToBeFusedBuilder.addVertex(v, groupedDAG);
      groupedDAG.getIncomingEdgesOf(v).forEach(dagToBeFusedBuilder::connectVertices);

      dagNotToBeFusedBuilder.addVertex(v, groupedDAG);
      groupedDAG.getIncomingEdgesOf(v).forEach(dagNotToBeFusedBuilder::connectVertices);
    });
    final Optional<LoopVertex> loopInDAG = groupedDAG.getTopologicalSort().stream()
        .filter(irVertex -> irVertex instanceof LoopVertex).map(irVertex -> (LoopVertex) irVertex).findFirst();
    assertTrue(loopInDAG.isPresent());

    final IRVertex vertexFollowedByLoop = groupedDAG.getIncomingEdgesOf(loopInDAG.get()).get(0).getSrc();

    // We're going to put this additional loop to the DAG, to test out the LoopFusion.
    final LoopVertex newLoop = loopInDAG.get().getClone();
    addLoopVertexToBuilder(dagToBeFusedBuilder, vertexFollowedByLoop, newLoop);
    dagToBeFused = dagToBeFusedBuilder.build();

    // additional Loop with different condition.
    final LoopVertex newLoopWithDiffCondition = loopInDAG.get().getClone();
    newLoopWithDiffCondition.setTerminationCondition((i) -> i < 100);
    addLoopVertexToBuilder(dagNotToBeFusedBuilder, vertexFollowedByLoop, newLoopWithDiffCondition);
    dagNotToBeFused = dagNotToBeFusedBuilder.build();

    // partially fused: two and one.
    addLoopVertexToBuilder(dagToBeFusedBuilder, newLoop, newLoopWithDiffCondition);
    dagToBePartiallyFused = dagToBeFusedBuilder.build();

    // partially fused2: two and two.
    final LoopVertex newLoopWithDiffDiffCondition = newLoopWithDiffCondition.getClone();
    newLoopWithDiffDiffCondition.setTerminationCondition((i) -> i < 100);
    addLoopVertexToBuilder(dagToBeFusedBuilder, loopInDAG.get(), newLoopWithDiffDiffCondition);
    dagToBePartiallyFused2 = dagToBeFusedBuilder.build();

    // partially fused3: due to dependency - two and two and one.
    addLoopVertexToBuilder(dagToBeFusedBuilder, newLoopWithDiffCondition, newLoopWithDiffCondition.getClone());
    dagToBePartiallyFused3 = dagToBeFusedBuilder.build();
  }

  /**
   * This method adds a LoopVertex at the end of the DAG (no more outgoing edges), after the
   * {@param vertexToBeFollowed}. We assume, as in the MLR, ALS DAG, that iterative incoming edges work to receive
   * main inputs, and non-iterative incoming edges work to receive side inputs.
   * @param builder builder to add the LoopVertex to.
   * @param vertexToBeFollowed vertex that is to be followed by the LoopVertex.
   * @param loopVertexToFollow the new LoopVertex that will be added.
   */
  private static void addLoopVertexToBuilder(final DAGBuilder<IRVertex, IREdge> builder,
                                             final IRVertex vertexToBeFollowed,
                                             final LoopVertex loopVertexToFollow) {
    builder.addVertex(loopVertexToFollow);
    loopVertexToFollow.getIterativeIncomingEdges().values().forEach(irEdges -> irEdges.forEach(irEdge -> {
      final IREdge newIREdge = new IREdge(irEdge.getProperty(ExecutionProperty.Key.DataCommunicationPattern),
          vertexToBeFollowed, loopVertexToFollow, irEdge.getCoder());
      builder.connectVertices(newIREdge);
    }));
    loopVertexToFollow.getNonIterativeIncomingEdges().values().forEach(irEdges -> irEdges.forEach(irEdge -> {
      final IREdge newIREdge = new IREdge(irEdge.getProperty(ExecutionProperty.Key.DataCommunicationPattern),
          irEdge.getSrc(), loopVertexToFollow, irEdge.getCoder());
      builder.connectVertices(newIREdge);
    }));
  }

  @Test
  public void testLoopFusionPass() throws Exception {
    final long numberOfGroupedVertices = groupedDAG.getVertices().size();
    final DAG<IRVertex, IREdge> processedDAG = LoopOptimizations.getLoopFusionPass().apply(dagToBeFused);
    assertEquals(numberOfGroupedVertices, processedDAG.getVertices().size());

    // no loop
    final long numberOfOriginalVertices = originalALSDAG.getVertices().size();
    final DAG<IRVertex, IREdge> processedNoLoopDAG =
        LoopOptimizations.getLoopFusionPass().apply(originalALSDAG);
    assertEquals(numberOfOriginalVertices, processedNoLoopDAG.getVertices().size());

    // one loop
    final DAG<IRVertex, IREdge> processedOneLoopDAG = LoopOptimizations.getLoopFusionPass().apply(groupedDAG);
    assertEquals(numberOfGroupedVertices, processedOneLoopDAG.getVertices().size());

    // not to be fused loops
    final long numberOfNotToBeFusedVertices = dagNotToBeFused.getVertices().size();
    final DAG<IRVertex, IREdge> processedNotToBeFusedDAG =
        LoopOptimizations.getLoopFusionPass().apply(dagNotToBeFused);
    assertEquals(numberOfNotToBeFusedVertices, processedNotToBeFusedDAG.getVertices().size());

    // to be partially fused loops: two and one
    final DAG<IRVertex, IREdge> processedToBePartiallyFusedDAG =
        LoopOptimizations.getLoopFusionPass().apply(dagToBePartiallyFused);
    assertEquals(numberOfNotToBeFusedVertices, processedToBePartiallyFusedDAG.getVertices().size());

    // to be partially fused loops: two and two
    final DAG<IRVertex, IREdge> processedToBePartiallyFusedDAG2 =
        LoopOptimizations.getLoopFusionPass().apply(dagToBePartiallyFused2);
    assertEquals(numberOfNotToBeFusedVertices, processedToBePartiallyFusedDAG2.getVertices().size());

    // to be partially fused, due to dependency: two and two and one
    final DAG<IRVertex, IREdge> processedToBePartiallyFusedDAG3 =
        LoopOptimizations.getLoopFusionPass().apply(dagToBePartiallyFused3);
    assertEquals(numberOfNotToBeFusedVertices + 1, processedToBePartiallyFusedDAG3.getVertices().size());
  }
}
