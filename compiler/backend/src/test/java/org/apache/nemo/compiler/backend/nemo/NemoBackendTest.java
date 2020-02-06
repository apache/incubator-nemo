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
package org.apache.nemo.compiler.backend.nemo;

import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.test.EmptyComponents;
import org.apache.nemo.compiler.optimizer.policy.TransientResourcePolicy;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Before;
import org.junit.Test;

import static org.apache.nemo.common.dag.DAG.EMPTY_DAG_DIRECTORY;
import static org.junit.Assert.assertEquals;

/**
 * Test NemoBackend.
 */
public final class NemoBackendTest<I, O> {
  private final IRVertex source = new EmptyComponents.EmptySourceVertex<>("Source");
  private final IRVertex map1 = new OperatorVertex(new EmptyComponents.EmptyTransform("MapElements"));
  private final IRVertex groupByKey = new OperatorVertex(new EmptyComponents.EmptyTransform("GroupByKey"));
  private final IRVertex combine = new OperatorVertex(new EmptyComponents.EmptyTransform("Combine"));
  private final IRVertex map2 = new OperatorVertex(new EmptyComponents.EmptyTransform("MapElements2"));
  private NemoBackend nemoBackend;

  private final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
  private IRDAG dag;

  @Before
  public void setUp() throws Exception {
    this.dag = new IRDAG(builder.addVertex(source).addVertex(map1).addVertex(groupByKey).addVertex(combine).addVertex(map2)
      .connectVertices(new IREdge(CommunicationPatternProperty.Value.ONE_TO_ONE, source, map1))
      .connectVertices(EmptyComponents.newDummyShuffleEdge(map1, groupByKey))
      .connectVertices(new IREdge(CommunicationPatternProperty.Value.ONE_TO_ONE, groupByKey, combine))
      .connectVertices(new IREdge(CommunicationPatternProperty.Value.ONE_TO_ONE, combine, map2))
      .build());

    this.dag = new TransientResourcePolicy().runCompileTimeOptimization(dag, EMPTY_DAG_DIRECTORY);

    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(JobConf.DAGDirectory.class, "");
    this.nemoBackend = injector.getInstance(NemoBackend.class);
  }

  /**
   * This method uses an IR DAG and tests whether NemoBackend successfully generates an Execution Plan.
   */
  @Test
  public void testExecutionPlanGeneration() {
    final PhysicalPlan executionPlan = nemoBackend.compile(dag);

    assertEquals(2, executionPlan.getStageDAG().getVertices().size());
    assertEquals(2, executionPlan.getStageDAG().getTopologicalSort().get(0).getIRDAG().getVertices().size());
    assertEquals(3, executionPlan.getStageDAG().getTopologicalSort().get(1).getIRDAG().getVertices().size());
  }
}
