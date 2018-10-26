package org.apache.nemo.examples.beam.policy;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.optimizer.policy.LargeShufflePolicy;
import org.apache.nemo.compiler.optimizer.policy.Policy;
import org.apache.nemo.compiler.optimizer.policy.PolicyImpl;
import org.apache.reef.tang.Injector;

/**
 * A large shuffle policy with fixed parallelism 5 for tests.
 */
public final class LargeShufflePolicyParallelismFive implements Policy {
  private final Policy policy;

  public LargeShufflePolicyParallelismFive() {
    this.policy = new PolicyImpl(
        PolicyTestUtil.overwriteParallelism(5, LargeShufflePolicy.BUILDER.getCompileTimePasses()),
        LargeShufflePolicy.BUILDER.getRuntimePasses());
  }

  @Override
  public DAG<IRVertex, IREdge> runCompileTimeOptimization(final DAG<IRVertex, IREdge> dag, final String dagDirectory) {
    return this.policy.runCompileTimeOptimization(dag, dagDirectory);
  }

  @Override
  public void registerRunTimeOptimizations(final Injector injector, final PubSubEventHandlerWrapper pubSubWrapper) {
    this.policy.registerRunTimeOptimizations(injector, pubSubWrapper);
  }
}
