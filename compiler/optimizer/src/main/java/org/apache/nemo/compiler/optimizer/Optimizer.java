package org.apache.nemo.compiler.optimizer;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * An interface for optimizer, which manages the optimization over submitted IR DAGs through
 * {@link org.apache.nemo.compiler.optimizer.policy.Policy}s.
 */
@DefaultImplementation(NemoOptimizer.class)
public interface Optimizer {

  /**
   * Optimize the submitted DAG.
   *
   * @param dag the input DAG to optimize.
   * @return optimized DAG, reshaped or tagged with execution properties.
   */
  DAG<IRVertex, IREdge> optimizeDag(DAG<IRVertex, IREdge> dag);
}
