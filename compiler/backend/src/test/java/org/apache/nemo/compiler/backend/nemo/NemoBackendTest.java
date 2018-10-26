package org.apache.nemo.compiler.backend.nemo;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.dag.DAGBuilder;
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
  private DAG<IRVertex, IREdge> dag;

  @Before
  public void setUp() throws Exception {
    this.dag = builder.addVertex(source).addVertex(map1).addVertex(groupByKey).addVertex(combine).addVertex(map2)
        .connectVertices(new IREdge(CommunicationPatternProperty.Value.OneToOne, source, map1))
        .connectVertices(new IREdge(CommunicationPatternProperty.Value.Shuffle, map1, groupByKey))
        .connectVertices(new IREdge(CommunicationPatternProperty.Value.OneToOne, groupByKey, combine))
        .connectVertices(new IREdge(CommunicationPatternProperty.Value.OneToOne, combine, map2))
        .build();

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
