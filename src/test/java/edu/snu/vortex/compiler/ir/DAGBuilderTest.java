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
package edu.snu.vortex.compiler.ir;

import edu.snu.vortex.compiler.TestUtil;
import edu.snu.vortex.compiler.frontend.beam.BoundedSourceVertex;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Tests for DAGBuilder.
 */
public class DAGBuilderTest {
  private final Vertex source = new BoundedSourceVertex<>(new TestUtil.EmptyBoundedSource("Source"));
  private final Vertex map1 = new OperatorVertex(new TestUtil.EmptyTransform("MapElements"));
  private final Vertex groupByKey = new OperatorVertex(new TestUtil.EmptyTransform("GroupByKey"));
  private final Vertex combine = new OperatorVertex(new TestUtil.EmptyTransform("Combine"));
  private final Vertex map2 = new OperatorVertex(new TestUtil.EmptyTransform("MapElements"));

  @Test
  public void addVertex() {
    final DAGBuilder builder = new DAGBuilder();

    builder.addVertex(source);
    assertTrue(builder.contains(source));

    final DAG dag = builder.build();
    assertTrue(dag.contains(source));
  }

  @Test
  public void connectVertices() {
    final DAGBuilder builder = new DAGBuilder();

    builder.addVertex(source);
    builder.addVertex(map1);
    builder.connectVertices(source, map1, Edge.Type.OneToOne);
    final DAG dag = builder.build();

    assertTrue(dag.getInEdgesOf(map1).isPresent());
    dag.getInEdgesOf(map1).ifPresent(edges -> edges.forEach(edge -> assertTrue(dag.contains(edge))));
    assertTrue(dag.getOutEdgesOf(source).isPresent());
    dag.getOutEdgesOf(source).ifPresent(edges -> edges.forEach(edge -> assertTrue(dag.contains(edge))));
  }

  @Test
  public void buildMapReduce() {
    final DAGBuilder builder = new DAGBuilder();

    builder.addVertex(source);
    builder.addVertex(map1);
    builder.addVertex(groupByKey);
    builder.addVertex(combine);
    builder.addVertex(map2);

    builder.connectVertices(source, map1, Edge.Type.OneToOne);
    builder.connectVertices(map1, groupByKey, Edge.Type.ScatterGather);
    builder.connectVertices(groupByKey, combine, Edge.Type.OneToOne);
    builder.connectVertices(combine, map2, Edge.Type.OneToOne);

    final DAG dag = builder.build();

    assertTrue(dag.getInEdgesOf(map1).isPresent());
    dag.getInEdgesOf(map1).ifPresent(edges -> edges.forEach(edge -> assertTrue(dag.contains(edge))));
    assertTrue(dag.getInEdgesOf(groupByKey).isPresent());
    dag.getInEdgesOf(groupByKey).ifPresent(edges -> edges.forEach(edge -> assertTrue(dag.contains(edge))));
    assertTrue(dag.getInEdgesOf(combine).isPresent());
    dag.getInEdgesOf(combine).ifPresent(edges -> edges.forEach(edge -> assertTrue(dag.contains(edge))));
    assertTrue(dag.getInEdgesOf(map2).isPresent());
    dag.getInEdgesOf(map2).ifPresent(edges -> edges.forEach(edge -> assertTrue(dag.contains(edge))));
    assertTrue(dag.getOutEdgesOf(source).isPresent());
    dag.getOutEdgesOf(source).ifPresent(edges -> edges.forEach(edge -> assertTrue(dag.contains(edge))));
    assertTrue(dag.getOutEdgesOf(map1).isPresent());
    dag.getOutEdgesOf(map1).ifPresent(edges -> edges.forEach(edge -> assertTrue(dag.contains(edge))));
    assertTrue(dag.getOutEdgesOf(groupByKey).isPresent());
    dag.getOutEdgesOf(groupByKey).ifPresent(edges -> edges.forEach(edge -> assertTrue(dag.contains(edge))));
    assertTrue(dag.getOutEdgesOf(combine).isPresent());
    dag.getOutEdgesOf(combine).ifPresent(edges -> edges.forEach(edge -> assertTrue(dag.contains(edge))));

    assertTrue(dag.contains(source));
    assertTrue(dag.contains(map1));
    assertTrue(dag.contains(groupByKey));
    assertTrue(dag.contains(combine));
    assertTrue(dag.contains(map2));

    assertTrue(builder.size() == dag.size());
    assertTrue(dag.size() == 5);
  }
}
