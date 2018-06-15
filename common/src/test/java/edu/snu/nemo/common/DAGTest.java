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
package edu.snu.nemo.common;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.Edge;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.dag.Vertex;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link DAG} and {@link DAGBuilder}.
 * This class tests the basic methods to build a DAG using {@link DAGBuilder}
 * and the basic methods to retrieve components of the DAG using {@link DAG}.
 */
public final class DAGTest {
  private DAGBuilder<IntegerVertex, Edge<IntegerVertex>> dagBuilder;

  @Before
  public void setUp() {
    dagBuilder = new DAGBuilder<>();
    dagBuilder.addVertex(new IntegerVertex(1));
    dagBuilder.addVertex(new IntegerVertex(2));
    dagBuilder.addVertex(new IntegerVertex(3));
    dagBuilder.addVertex(new IntegerVertex(4));
    dagBuilder.addVertex(new IntegerVertex(5));
  }

  @Test
  public void testSimpleDAG() {
    final Edge<IntegerVertex> e1 = new Edge<>("1", new IntegerVertex(1), new IntegerVertex(2));
    final Edge<IntegerVertex> e2 = new Edge<>("2", new IntegerVertex(2), new IntegerVertex(3));
    final Edge<IntegerVertex> e3 = new Edge<>("3", new IntegerVertex(3), new IntegerVertex(4));
    final Edge<IntegerVertex> e4 = new Edge<>("4", new IntegerVertex(4), new IntegerVertex(5));

    dagBuilder.connectVertices(e1);
    dagBuilder.connectVertices(e2);
    dagBuilder.connectVertices(e3);
    dagBuilder.connectVertices(e4);

    final DAG<IntegerVertex, Edge<IntegerVertex>> dag = dagBuilder.build();

    assertEquals(dag.getVertices().size(), 5);
    assertEquals(dag.getIncomingEdgesOf(new IntegerVertex(1)).size(), 0);
    assertEquals(dag.getOutgoingEdgesOf(new IntegerVertex(5)).size(), 0);
    assertEquals(dag.getIncomingEdgesOf(new IntegerVertex(3)).size(), 1);
    assertEquals(dag.getOutgoingEdgesOf(new IntegerVertex(4)).size(), 1);
    assertEquals(dag.getTopologicalSort().size(), 5);

    final List<IntegerVertex> topologicalOrder = dag.getTopologicalSort();
    assertEquals(topologicalOrder.get(0).getValue(), 1);
    assertEquals(topologicalOrder.get(1).getValue(), 2);
    assertEquals(topologicalOrder.get(2).getValue(), 3);
    assertEquals(topologicalOrder.get(3).getValue(), 4);
    assertEquals(topologicalOrder.get(4).getValue(), 5);

    assertEquals(dag.getRootVertices().size(), 1);
    assertEquals(dag.getParents("1").size(), 0);
    assertEquals(dag.getChildren("1").size(), 1);
    assertEquals(dag.getParents("2").size(), 1);
    assertEquals(dag.getChildren("2").size(), 1);
    assertEquals(dag.getParents("5").size(), 1);
    assertEquals(dag.getChildren("5").size(), 0);
    assertEquals(dag.getVertexById("1"), new IntegerVertex(1));
  }

  @Test
  public void testNormalDAG() {
    final Edge<IntegerVertex> e1 = new Edge<>("1", new IntegerVertex(1), new IntegerVertex(2));
    final Edge<IntegerVertex> e2 = new Edge<>("2", new IntegerVertex(2), new IntegerVertex(3));
    final Edge<IntegerVertex> e3 = new Edge<>("3", new IntegerVertex(4), new IntegerVertex(5));
    final Edge<IntegerVertex> e4 = new Edge<>("4", new IntegerVertex(4), new IntegerVertex(3));

    dagBuilder.connectVertices(e1);
    dagBuilder.connectVertices(e2);
    dagBuilder.connectVertices(e3);
    dagBuilder.connectVertices(e4);

    final DAG<IntegerVertex, Edge<IntegerVertex>> dag = dagBuilder.build();

    assertEquals(dag.getOutgoingEdgesOf(new IntegerVertex(4)).size(), 2);
    assertEquals(dag.getIncomingEdgesOf(new IntegerVertex(3)).size(), 2);

    final List<IntegerVertex> topologicalOrder = dag.getTopologicalSort();
    assertEquals(topologicalOrder.get(0).getValue(), 4);
    assertEquals(topologicalOrder.get(1).getValue(), 5);
    assertEquals(topologicalOrder.get(2).getValue(), 1);
    assertEquals(topologicalOrder.get(3).getValue(), 2);
    assertEquals(topologicalOrder.get(4).getValue(), 3);

    assertEquals(dag.getRootVertices().size(), 2);
    assertEquals(dag.getParents("4").size(), 0);
    assertEquals(dag.getChildren("4").size(), 2);
    assertEquals(dag.getParents("3").size(), 2);
    assertEquals(dag.getChildren("3").size(), 0);
    assertEquals(dag.getParents("5").size(), 1);
    assertEquals(dag.getChildren("5").size(), 0);
    assertEquals(dag.getVertexById("3"), new IntegerVertex(3));

    List<IntegerVertex> ancestors = dag.getAncestors("5");
    assertEquals(ancestors.size(), 1);
    assertTrue(ancestors.contains(new IntegerVertex(4)));

    ancestors = dag.getAncestors("3");
    assertEquals(ancestors.size(), 3);
    assertTrue(ancestors.contains(new IntegerVertex(1)));
    assertTrue(ancestors.contains(new IntegerVertex(2)));
    assertTrue(ancestors.contains(new IntegerVertex(4)));

    List<IntegerVertex> descendants = dag.getDescendants("4");
    assertEquals(descendants.size(), 2);
    assertTrue(descendants.contains(new IntegerVertex(3)));
    assertTrue(descendants.contains(new IntegerVertex(5)));

    descendants = dag.getDescendants("5");
    assertEquals(descendants.size(), 0);

    descendants = dag.getDescendants("2");
    assertEquals(descendants.size(), 1);
    assertTrue(descendants.contains(new IntegerVertex(3)));
  }

  final class IntegerVertex extends Vertex {
    private final int value;
    public IntegerVertex(final int value) {
      super(String.valueOf(value));
      this.value = value;
    }

    public int getValue() {
      return value;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()){
        return false;
      }
      final IntegerVertex that = (IntegerVertex) o;
      return value == that.value;
    }

    @Override
    public int hashCode() {
      return value;
    }
  }
}
