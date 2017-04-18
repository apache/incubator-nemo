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
package edu.snu.vortex.utils;

import edu.snu.vortex.utils.dag.DAG;
import edu.snu.vortex.utils.dag.Edge;
import edu.snu.vortex.utils.dag.DAGBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests {@link DAG} and {@link DAGBuilder}.
 * This class tests the basic methods to build a DAG using {@link DAGBuilder}
 * and the basic methods to retrieve components of the DAG using {@link DAG}.
 */
public final class DAGTest {
  private DAGBuilder<Integer, Edge<Integer>> dagBuilder;

  @Before
  public void setUp() {
    dagBuilder = new DAGBuilder<>();
    dagBuilder.addVertex(1);
    dagBuilder.addVertex(2);
    dagBuilder.addVertex(3);
    dagBuilder.addVertex(4);
    dagBuilder.addVertex(5);
  }

  @Test
  public void testSimpleDAG() {
    final Edge<Integer> e1 = new Edge<>(1, 2);
    final Edge<Integer> e2 = new Edge<>(2, 3);
    final Edge<Integer> e3 = new Edge<>(3, 4);
    final Edge<Integer> e4 = new Edge<>(4, 5);

    dagBuilder.connectVertices(e1);
    dagBuilder.connectVertices(e2);
    dagBuilder.connectVertices(e3);
    dagBuilder.connectVertices(e4);

    final DAG<Integer, Edge<Integer>> dag = dagBuilder.build();

    assertEquals(dag.getVertices().size(), 5);
    assertEquals(dag.getIncomingEdgesOf(1).size(), 0);
    assertEquals(dag.getOutgoingEdgesOf(5).size(), 0);
    assertEquals(dag.getIncomingEdgesOf(3).size(), 1);
    assertEquals(dag.getOutgoingEdgesOf(4).size(), 1);
    assertEquals(dag.getTopologicalSort().size(), 5);

    final List<Integer> topologicalOrder = dag.getTopologicalSort();
    assertEquals((int) topologicalOrder.get(0), 1);
    assertEquals((int) topologicalOrder.get(1), 2);
    assertEquals((int) topologicalOrder.get(2), 3);
    assertEquals((int) topologicalOrder.get(3), 4);
    assertEquals((int) topologicalOrder.get(4), 5);
  }

  @Test
  public void testNormalDAG() {
    final Edge<Integer> e1 = new Edge<>(1, 2);
    final Edge<Integer> e2 = new Edge<>(2, 3);
    final Edge<Integer> e3 = new Edge<>(4, 5);
    final Edge<Integer> e4 = new Edge<>(4, 3);

    dagBuilder.connectVertices(e1);
    dagBuilder.connectVertices(e2);
    dagBuilder.connectVertices(e3);
    dagBuilder.connectVertices(e4);

    final DAG<Integer, Edge<Integer>> dag = dagBuilder.build();

    assertEquals(dag.getOutgoingEdgesOf(4).size(), 2);
    assertEquals(dag.getIncomingEdgesOf(3).size(), 2);

    final List<Integer> topologicalOrder = dag.getTopologicalSort();
    assertEquals((int) topologicalOrder.get(0), 4);
    assertEquals((int) topologicalOrder.get(1), 5);
    assertEquals((int) topologicalOrder.get(2), 1);
    assertEquals((int) topologicalOrder.get(3), 2);
    assertEquals((int) topologicalOrder.get(4), 3);
  }
}
