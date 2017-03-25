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
package edu.snu.vortex.compiler.backend.vortex;

import edu.snu.vortex.compiler.TestUtil;
import edu.snu.vortex.compiler.backend.Backend;
import edu.snu.vortex.compiler.frontend.beam.BoundedSourceVertex;
import edu.snu.vortex.compiler.ir.*;
import edu.snu.vortex.compiler.optimizer.Optimizer;
import edu.snu.vortex.runtime.common.plan.logical.ExecutionPlan;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

/**
 * Test Vortex Backend.
 */
public final class VortexBackendTest {
  private final Vertex source = new BoundedSourceVertex<>(new TestUtil.EmptyBoundedSource("Source"));
  private final Vertex map1 = new OperatorVertex(new TestUtil.EmptyTransform("MapElements"));
  private final Vertex groupByKey = new OperatorVertex(new TestUtil.EmptyTransform("GroupByKey"));
  private final Vertex combine = new OperatorVertex(new TestUtil.EmptyTransform("Combine"));
  private final Vertex map2 = new OperatorVertex(new TestUtil.EmptyTransform("MapElements"));

  private final DAGBuilder builder = new DAGBuilder();
  private DAG dag;

  @Before
  public void setUp() throws Exception {
    builder.addVertex(source);
    builder.addVertex(map1);
    builder.addVertex(groupByKey);
    builder.addVertex(combine);
    builder.addVertex(map2);
    builder.connectVertices(source, map1, Edge.Type.OneToOne);
    builder.connectVertices(map1, groupByKey, Edge.Type.ScatterGather);
    builder.connectVertices(groupByKey, combine, Edge.Type.OneToOne);
    builder.connectVertices(combine, map2, Edge.Type.OneToOne);

    this.dag = builder.build();
    this.dag = new Optimizer().optimize(dag, Optimizer.PolicyType.Pado);
  }

  @Test
  public void test() throws Exception {
    final Backend<ExecutionPlan> backend = new VortexBackend();
    final ExecutionPlan executionPlan = backend.compile(dag);

    assertTrue(executionPlan.getRuntimeStages().size() == 2);
    assertTrue(executionPlan.getRuntimeStages().get(0).getRuntimeVertices().size() == 2);
    assertTrue(executionPlan.getRuntimeStages().get(1).getRuntimeVertices().size() == 3);
  }
}
