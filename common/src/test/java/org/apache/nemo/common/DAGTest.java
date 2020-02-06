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
package org.apache.nemo.common;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.dag.Vertex;
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

    assertEquals(5, dag.getVertices().size());
    assertEquals(0, dag.getIncomingEdgesOf(new IntegerVertex(1)).size());
    assertEquals(0, dag.getOutgoingEdgesOf(new IntegerVertex(5)).size());
    assertEquals(1, dag.getIncomingEdgesOf(new IntegerVertex(3)).size());
    assertEquals(1, dag.getOutgoingEdgesOf(new IntegerVertex(4)).size());
    assertEquals(5, dag.getTopologicalSort().size());

    final List<IntegerVertex> topologicalOrder = dag.getTopologicalSort();

    assertEquals(1, topologicalOrder.get(0).getValue());
    assertEquals(2, topologicalOrder.get(1).getValue());
    assertEquals(3, topologicalOrder.get(2).getValue());
    assertEquals(4, topologicalOrder.get(3).getValue());
    assertEquals(5, topologicalOrder.get(4).getValue());

    assertEquals(1, dag.getRootVertices().size());
    assertEquals(0, dag.getParents("1").size());
    assertEquals(1, dag.getChildren("1").size());
    assertEquals(1, dag.getParents("2").size());
    assertEquals(1, dag.getChildren("2").size());
    assertEquals(1, dag.getParents("5").size());
    assertEquals(0, dag.getChildren("5").size());
    assertEquals(new IntegerVertex(1), dag.getVertexById("1"));
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

    assertEquals(2, dag.getOutgoingEdgesOf(new IntegerVertex(4)).size());
    assertEquals(2, dag.getIncomingEdgesOf(new IntegerVertex(3)).size());

    final List<IntegerVertex> topologicalOrder = dag.getTopologicalSort();
    assertEquals(4, topologicalOrder.get(0).getValue());
    assertEquals(5, topologicalOrder.get(1).getValue());
    assertEquals(1, topologicalOrder.get(2).getValue());
    assertEquals(2, topologicalOrder.get(3).getValue());
    assertEquals(3, topologicalOrder.get(4).getValue());

    assertEquals(2, dag.getRootVertices().size());
    assertEquals(0, dag.getParents("4").size());
    assertEquals(2, dag.getChildren("4").size());
    assertEquals(2, dag.getParents("3").size());
    assertEquals(0, dag.getChildren("3").size());
    assertEquals(1, dag.getParents("5").size());
    assertEquals(0, dag.getChildren("5").size());
    assertEquals(new IntegerVertex(3), dag.getVertexById("3"));

    List<IntegerVertex> ancestors = dag.getAncestors("5");
    assertEquals(1, ancestors.size());
    assertTrue(ancestors.contains(new IntegerVertex(4)));

    ancestors = dag.getAncestors("3");
    assertEquals(3, ancestors.size());
    assertTrue(ancestors.contains(new IntegerVertex(1)));
    assertTrue(ancestors.contains(new IntegerVertex(2)));
    assertTrue(ancestors.contains(new IntegerVertex(4)));

    List<IntegerVertex> descendants = dag.getDescendants("4");
    assertEquals(2, descendants.size());
    assertTrue(descendants.contains(new IntegerVertex(3)));
    assertTrue(descendants.contains(new IntegerVertex(5)));

    descendants = dag.getDescendants("5");
    assertEquals(0, descendants.size());

    descendants = dag.getDescendants("2");
    assertEquals(1, descendants.size());
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
      if (o == null || getClass() != o.getClass()) {
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
