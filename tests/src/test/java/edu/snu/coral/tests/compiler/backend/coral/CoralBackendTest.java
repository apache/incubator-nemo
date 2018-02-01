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
package edu.snu.coral.tests.compiler.backend.coral;

import edu.snu.coral.common.dag.DAG;
import edu.snu.coral.common.ir.edge.IREdge;
import edu.snu.coral.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.coral.common.ir.vertex.IRVertex;
import edu.snu.coral.common.ir.vertex.OperatorVertex;
import edu.snu.coral.common.coder.Coder;
import edu.snu.coral.compiler.backend.coral.CoralBackend;
import edu.snu.coral.common.dag.DAGBuilder;
import edu.snu.coral.compiler.optimizer.CompiletimeOptimizer;
import edu.snu.coral.compiler.optimizer.examples.EmptyComponents;
import edu.snu.coral.compiler.optimizer.policy.PadoPolicy;
import edu.snu.coral.conf.JobConf;
import edu.snu.coral.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.coral.runtime.common.plan.physical.PhysicalPlanGenerator;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Before;
import org.junit.Test;

import static edu.snu.coral.common.dag.DAG.EMPTY_DAG_DIRECTORY;
import static org.junit.Assert.assertEquals;

/**
 * Test CoralBackend.
 */
public final class CoralBackendTest<I, O> {
  private final IRVertex source = new EmptyComponents.EmptySourceVertex<>("Source");
  private final IRVertex map1 = new OperatorVertex(new EmptyComponents.EmptyTransform("MapElements"));
  private final IRVertex groupByKey = new OperatorVertex(new EmptyComponents.EmptyTransform("GroupByKey"));
  private final IRVertex combine = new OperatorVertex(new EmptyComponents.EmptyTransform("Combine"));
  private final IRVertex map2 = new OperatorVertex(new EmptyComponents.EmptyTransform("MapElements2"));
  private PhysicalPlanGenerator physicalPlanGenerator;

  private final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
  private DAG<IRVertex, IREdge> dag;

  @Before
  public void setUp() throws Exception {
    this.dag = builder.addVertex(source).addVertex(map1).addVertex(groupByKey).addVertex(combine).addVertex(map2)
        .connectVertices(new IREdge(DataCommunicationPatternProperty.Value.OneToOne, source, map1, Coder.DUMMY_CODER))
        .connectVertices(new IREdge(DataCommunicationPatternProperty.Value.Shuffle,
            map1, groupByKey, Coder.DUMMY_CODER))
        .connectVertices(new IREdge(DataCommunicationPatternProperty.Value.OneToOne,
            groupByKey, combine, Coder.DUMMY_CODER))
        .connectVertices(new IREdge(DataCommunicationPatternProperty.Value.OneToOne, combine, map2, Coder.DUMMY_CODER))
        .build();

    this.dag = CompiletimeOptimizer.optimize(dag, new PadoPolicy(), EMPTY_DAG_DIRECTORY);

    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(JobConf.DAGDirectory.class, "");
    this.physicalPlanGenerator = injector.getInstance(PhysicalPlanGenerator.class);
  }

  /**
   * This method uses an IR DAG and tests whether CoralBackend successfully generates an Execution Plan.
   * @throws Exception during the Execution Plan generation.
   */
  @Test
  public void testExecutionPlanGeneration() {
    final CoralBackend backend = new CoralBackend();
    final PhysicalPlan executionPlan = backend.compile(dag, physicalPlanGenerator);

    assertEquals(2, executionPlan.getStageDAG().getVertices().size());
    assertEquals(1, executionPlan.getStageDAG().getTopologicalSort().get(0).getTaskGroupIds().size());
    assertEquals(1, executionPlan.getStageDAG().getTopologicalSort().get(1).getTaskGroupIds().size());
  }
}
