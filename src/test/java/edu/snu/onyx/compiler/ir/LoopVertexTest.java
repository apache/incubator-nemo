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
package edu.snu.onyx.compiler.ir;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.compiler.frontend.beam.BoundedSourceVertex;
import edu.snu.onyx.compiler.frontend.beam.transform.DoTransform;
import edu.snu.onyx.common.Pair;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.dag.DAGBuilder;
import edu.snu.onyx.compiler.optimizer.examples.EmptyComponents;
import edu.snu.onyx.runtime.executor.datatransfer.communication.OneToOne;
import edu.snu.onyx.runtime.executor.datatransfer.communication.ScatterGather;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link LoopVertex}.
 */
public class LoopVertexTest {
  private final LoopVertex loopVertex = new LoopVertex("fakeTransform");
  private DAG<IRVertex, IREdge> originalDAG;
  private DAG<IRVertex, IREdge> newDAG;

  private final IRVertex source = new BoundedSourceVertex<>(new EmptyComponents.EmptyBoundedSource("Source"));
  private final IRVertex map1 = new OperatorVertex(new EmptyComponents.EmptyTransform("MapElements"));
  private final IRVertex groupByKey = new OperatorVertex(new EmptyComponents.EmptyTransform("GroupByKey"));
  private final IRVertex combine = new OperatorVertex(new EmptyComponents.EmptyTransform("Combine"));
  private final IRVertex map2 = new OperatorVertex(new DoTransform(null, null));

  @Before
  public void setUp() {
    final DAGBuilder<IRVertex, IREdge> loopDAGBuilder = loopVertex.getBuilder();
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    loopDAGBuilder.addVertex(map1).addVertex(groupByKey).addVertex(combine).addVertex(map2)
        .connectVertices(new IREdge(ScatterGather.class, map1, groupByKey, Coder.DUMMY_CODER))
        .connectVertices(new IREdge(OneToOne.class, groupByKey, combine, Coder.DUMMY_CODER))
        .connectVertices(new IREdge(OneToOne.class, combine, map2, Coder.DUMMY_CODER));
    loopVertex.addDagIncomingEdge(new IREdge(OneToOne.class, source, map1, Coder.DUMMY_CODER));
    loopVertex.addIterativeIncomingEdge(new IREdge(OneToOne.class, map2, map1, Coder.DUMMY_CODER));

    originalDAG = builder.addVertex(source).addVertex(map1).addVertex(groupByKey).addVertex(combine).addVertex(map2)
        .connectVertices(new IREdge(OneToOne.class, source, map1, Coder.DUMMY_CODER))
        .connectVertices(new IREdge(ScatterGather.class, map1, groupByKey, Coder.DUMMY_CODER))
        .connectVertices(new IREdge(OneToOne.class, groupByKey, combine, Coder.DUMMY_CODER))
        .connectVertices(new IREdge(OneToOne.class, combine, map2, Coder.DUMMY_CODER))
        .build();
  }

  @Test
  public void testDAG() {
    assertEquals(originalDAG.getTopologicalSort().size() - 1, loopVertex.getDAG().getTopologicalSort().size());
    // zip vertices
    final Iterator<IRVertex> vertices1 = originalDAG.getTopologicalSort().iterator();
    vertices1.next();
    final Iterator<IRVertex> vertices2 = loopVertex.getDAG().getTopologicalSort().iterator();
    final List<Pair<IRVertex, IRVertex>> list = new ArrayList<>();
    while  (vertices1.hasNext() && vertices2.hasNext()) {
      list.add(Pair.of(vertices1.next(), vertices2.next()));
    }
    list.forEach(irVertexPair -> {
        assertEquals(irVertexPair.left().getExecutionProperties(), irVertexPair.right().getExecutionProperties());
        assertEquals(originalDAG.getOutgoingEdgesOf(irVertexPair.left()).size(),
            loopVertex.getDAG().getOutgoingEdgesOf(irVertexPair.right()).size());
    });

    assertEquals(source, loopVertex.getDagIncomingEdges().values().iterator().next().iterator().next().getSrc());
    assertEquals(map1, loopVertex.getDagIncomingEdges().values().iterator().next().iterator().next().getDst());
  }

  @Test
  public void testIterationTermination() {
    final LoopVertex loopVertexClone = loopVertex.getClone();
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    builder.addVertex(source);

    assertEquals(false, loopVertexClone.loopTerminationConditionMet());
    assertEquals(true, loopVertexClone.unRollIteration(builder).loopTerminationConditionMet());

    loopVertexClone.setTerminationCondition((i) -> true);
    assertEquals(true, loopVertexClone.loopTerminationConditionMet());
  }
}
