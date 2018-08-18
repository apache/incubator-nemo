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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.reshaping;

import edu.snu.nemo.client.JobLauncher;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DecoderProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.EncoderProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.LoopVertex;
import edu.snu.nemo.compiler.CompilerTestUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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
    originalALSDAG = CompilerTestUtil.compileALSDAG();
    groupedDAG = new LoopExtractionPass().apply(originalALSDAG);

    final Optional<LoopVertex> alsLoopOpt = groupedDAG.getTopologicalSort().stream()
        .filter(irVertex -> irVertex instanceof LoopVertex).map(irVertex -> (LoopVertex) irVertex).findFirst();
    assertTrue(alsLoopOpt.isPresent());
    final LoopVertex alsLoop = alsLoopOpt.get();

    final IRVertex vertex6 = groupedDAG.getTopologicalSort().get(11);
    final IRVertex vertex18 = alsLoop.getDAG().getTopologicalSort().get(4);

    final Set<IREdge> oldDAGIncomingEdges = alsLoop.getDagIncomingEdges().get(vertex18);
    final List<IREdge> newDAGIncomingEdge = groupedDAG.getIncomingEdgesOf(vertex6);

    alsLoop.getDagIncomingEdges().remove(vertex18);
    alsLoop.getDagIncomingEdges().putIfAbsent(vertex6, new HashSet<>());
    newDAGIncomingEdge.forEach(alsLoop.getDagIncomingEdges().get(vertex6)::add);

    alsLoop.getNonIterativeIncomingEdges().remove(vertex18);
    alsLoop.getNonIterativeIncomingEdges().putIfAbsent(vertex6, new HashSet<>());
    newDAGIncomingEdge.forEach(alsLoop.getNonIterativeIncomingEdges().get(vertex6)::add);

    alsLoop.getBuilder().addVertex(vertex6);
    oldDAGIncomingEdges.forEach(alsLoop.getBuilder()::connectVertices);

    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    groupedDAG.topologicalDo(v -> {
      if (!v.equals(vertex6) && !v.equals(alsLoop)) {
        builder.addVertex(v);
        groupedDAG.getIncomingEdgesOf(v).forEach(builder::connectVertices);
      } else if (v.equals(alsLoop)) {
        builder.addVertex(v);
        groupedDAG.getIncomingEdgesOf(v).forEach(e -> {
          if (!e.getSrc().equals(vertex6)) {
            builder.connectVertices(e);
          } else {
            final Optional<IREdge> incomingEdge = newDAGIncomingEdge.stream().findFirst();
            assertTrue(incomingEdge.isPresent());
            final IREdge newIREdge = new IREdge(incomingEdge.get().getPropertyValue(
                CommunicationPatternProperty.class).get(), incomingEdge.get().getSrc(), alsLoop);
            newIREdge.setProperty(EncoderProperty.of(incomingEdge.get().getPropertyValue(EncoderProperty.class).get()));
            newIREdge.setProperty(DecoderProperty.of(incomingEdge.get().getPropertyValue(DecoderProperty.class).get()));
            builder.connectVertices(newIREdge);
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
        .apply(dagToBeRefactored);
    assertEquals(numberOfGroupedVertices, processedDAG.getVertices().size());

    final DAG<IRVertex, IREdge> notProcessedDAG = LoopOptimizations.getLoopInvariantCodeMotionPass()
        .apply(groupedDAG);
    assertEquals(numberOfGroupedVertices, notProcessedDAG.getVertices().size());
  }

}
