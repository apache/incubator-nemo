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
package edu.snu.vortex.utils;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests {@link DAGImpl}
 */
public final class DAGImplTest {
  private DAG<Integer> dag;

  @Before
  public void setUp() {
    dag = new DAGImpl<>();
    dag.addVertex(1);
    dag.addVertex(2);
    dag.addVertex(3);
    dag.addVertex(4);
    dag.addVertex(5);
  }

  @Test
  public void testSimpleNormalDAG() {
    dag.addEdge(1, 2);
    dag.addEdge(2, 3);
    dag.addEdge(3, 4);
    dag.addEdge(4, 5);

    assertEquals(dag.getRootVertices().size(), 1);
    assertEquals(dag.getParents(1).size(), 0);
    assertEquals(dag.getParents(5).size(), 1);
    assertEquals(dag.getChildren(2).size(), 1);
    assertEquals(dag.getChildren(1).size(), 1);
  }

  @Test
  public void testNormalDAG() {
    dag.addEdge(1, 2);
    dag.addEdge(2, 3);
    dag.addEdge(4, 5);
    dag.addEdge(4, 3);

    assertEquals(dag.getRootVertices().size(), 2);
    assertEquals(dag.getParents(3).size(), 2);
    assertEquals(dag.getChildren(4).size(), 2);
  }

  @Test(expected = IllegalStateException.class)
  public void testCycleDetection() {
    dag.addEdge(1, 2);
    dag.addEdge(2, 3);
    dag.addEdge(3, 2);
  }

  @Test(expected = IllegalStateException.class)
  public void testCycleDetection2() {
    dag.addEdge(1, 2);
    dag.addEdge(2, 3);
    dag.addEdge(3, 1);
  }

  @Test
  public void testIndependentVertices() {
    dag.addEdge(1, 2);
    dag.addEdge(1, 3);
    dag.addEdge(4, 5);

    assertEquals(dag.getRootVertices().size(), 2);
  }

  @Test
  public void testIndependentVertices2() {
    testNormalDAG();

    dag.addEdge(1, 4);
    assertEquals(dag.getRootVertices().size(), 1);
  }
}
