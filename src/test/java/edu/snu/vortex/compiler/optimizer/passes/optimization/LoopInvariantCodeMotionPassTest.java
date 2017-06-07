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
package edu.snu.vortex.compiler.optimizer.passes.optimization;

import edu.snu.vortex.client.JobLauncher;
import edu.snu.vortex.compiler.TestUtil;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.LoopVertex;
import edu.snu.vortex.compiler.ir.OperatorVertex;
import edu.snu.vortex.compiler.optimizer.passes.LoopGroupingPass;
import edu.snu.vortex.utils.dag.DAG;
import edu.snu.vortex.utils.dag.DAGBuilder;
import edu.snu.vortex.utils.dag.Edge;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.swing.text.html.Option;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link LoopOptimizations.LoopInvariantCodeMotionPass}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JobLauncher.class)
public class LoopInvariantCodeMotionPassTest {
  private DAG<IRVertex, IREdge> originalALSDAG;
  private DAG<IRVertex, IREdge> groupedDAG;
  private DAG<IRVertex, IREdge> dagToBeRefactored;

  @Before
  public void setUp() throws Exception {
    originalALSDAG = TestUtil.compileALSDAG();
    groupedDAG = new LoopGroupingPass().process(originalALSDAG);

    final Optional<LoopVertex> alsLoopOpt = groupedDAG.getTopologicalSort().stream()
        .filter(irVertex -> irVertex instanceof LoopVertex).map(irVertex -> (LoopVertex) irVertex).findFirst();
    assertTrue(alsLoopOpt.isPresent());
    final LoopVertex alsLoop = alsLoopOpt.get();

    final IRVertex vertex7 = groupedDAG.getTopologicalSort().get(3);
    final IRVertex vertex11 = alsLoop.getDAG().getTopologicalSort().get(1);

    final Set<IREdge> oldDAGIncomingEdges = alsLoop.getDagIncomingEdges().get(vertex11);
    final List<IREdge> newDAGIncomingEdge = groupedDAG.getIncomingEdgesOf(vertex7);

    alsLoop.getDagIncomingEdges().remove(vertex11);
    alsLoop.getDagIncomingEdges().putIfAbsent(vertex7, new HashSet<>());
    newDAGIncomingEdge.forEach(alsLoop.getDagIncomingEdges().get(vertex7)::add);

    alsLoop.getNonIterativeIncomingEdges().remove(vertex11);
    alsLoop.getNonIterativeIncomingEdges().putIfAbsent(vertex7, new HashSet<>());
    newDAGIncomingEdge.forEach(alsLoop.getDagIncomingEdges().get(vertex7)::add);

    alsLoop.getBuilder().addVertex(vertex7);
    oldDAGIncomingEdges.forEach(alsLoop.getBuilder()::connectVertices);

    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    groupedDAG.topologicalDo(v -> {
      if (!v.equals(vertex7) && !v.equals(alsLoop)) {
        builder.addVertex(v);
        groupedDAG.getIncomingEdgesOf(v).forEach(builder::connectVertices);
      } else if (v.equals(alsLoop)) {
        builder.addVertex(v);
        groupedDAG.getIncomingEdgesOf(v).forEach(e -> {
          if (!e.getSrc().equals(vertex7)) {
            builder.connectVertices(e);
          } else {
            final Optional<IREdge> theIncomingEdge = newDAGIncomingEdge.stream().findFirst();
            assertTrue(theIncomingEdge.isPresent());
            final IREdge newIREdge = new IREdge(theIncomingEdge.get().getType(), theIncomingEdge.get().getSrc(),
                alsLoop, theIncomingEdge.get().getCoder());
          }
        });
      }
    });

    dagToBeRefactored = builder.build();
  }

  @Test
  public void testLoopInvariantCodeMotionPass() throws Exception {
    final long numberOfGroupedVertices = groupedDAG.getVertices().size();

    final DAG<IRVertex, IREdge> processedDAG = LoopOptimizations.getLoopInvariantCodeMotionPass()
        .process(dagToBeRefactored);
    assertEquals(numberOfGroupedVertices, processedDAG.getVertices().size());

    final DAG<IRVertex, IREdge> notProcessedDAG = LoopOptimizations.getLoopInvariantCodeMotionPass()
        .process(groupedDAG);
    assertEquals(numberOfGroupedVertices, notProcessedDAG.getVertices().size());
  }

}
