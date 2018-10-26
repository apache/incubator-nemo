package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ClonedSchedulingProperty;

/**
 * Speculative execution. (very aggressive, for unit tests)
 * TODO #200: Maintain Test Passes and Policies Separately
 */
@Annotates(ClonedSchedulingProperty.class)
public final class AggressiveSpeculativeCloningPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public AggressiveSpeculativeCloningPass() {
    super(AggressiveSpeculativeCloningPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    // Speculative execution policy.
    final double fractionToWaitFor = 0.00000001; // Aggressive
    final double medianTimeMultiplier = 1.00000001; // Aggressive

    // Apply the policy to ALL vertices
    dag.getVertices().forEach(vertex -> vertex.setProperty(ClonedSchedulingProperty.of(
      new ClonedSchedulingProperty.CloneConf(fractionToWaitFor, medianTimeMultiplier))));
    return dag;
  }
}
