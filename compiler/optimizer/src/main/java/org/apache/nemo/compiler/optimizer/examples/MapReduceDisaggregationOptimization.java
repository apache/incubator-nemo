package org.apache.nemo.compiler.optimizer.examples;

import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.test.EmptyComponents;
import org.apache.nemo.compiler.optimizer.policy.DisaggregationPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.nemo.common.dag.DAG.EMPTY_DAG_DIRECTORY;

/**
 * A sample MapReduceDisaggregationOptimization application.
 */
public final class MapReduceDisaggregationOptimization {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceDisaggregationOptimization.class.getName());

  /**
   * Private constructor.
   */
  private MapReduceDisaggregationOptimization() {
  }

  /**
   * Main function of the example MR program.
   * @param args arguments.
   * @throws Exception Exceptions on the way.
   */
  public static void main(final String[] args) throws Exception {
    final IRVertex source = new EmptyComponents.EmptySourceVertex<>("Source");
    final IRVertex map = new OperatorVertex(new EmptyComponents.EmptyTransform("MapVertex"));
    final IRVertex reduce = new OperatorVertex(new EmptyComponents.EmptyTransform("ReduceVertex"));

    // Before
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    builder.addVertex(source);
    builder.addVertex(map);
    builder.addVertex(reduce);

    final IREdge edge1 = new IREdge(CommunicationPatternProperty.Value.OneToOne, source, map);
    builder.connectVertices(edge1);

    final IREdge edge2 = new IREdge(CommunicationPatternProperty.Value.Shuffle, map, reduce);
    builder.connectVertices(edge2);

    final DAG<IRVertex, IREdge> dag = builder.build();
    LOG.info("Before Optimization");
    LOG.info(dag.toString());

    // Optimize
    final DAG optimizedDAG = new DisaggregationPolicy().runCompileTimeOptimization(dag, EMPTY_DAG_DIRECTORY);

    // After
    LOG.info("After Optimization");
    LOG.info(optimizedDAG.toString());
  }
}
