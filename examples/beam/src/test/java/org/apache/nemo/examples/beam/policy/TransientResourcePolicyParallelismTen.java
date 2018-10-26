package org.apache.nemo.examples.beam.policy;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.optimizer.policy.PolicyImpl;
import org.apache.nemo.compiler.optimizer.policy.TransientResourcePolicy;
import org.apache.nemo.compiler.optimizer.policy.Policy;
import org.apache.reef.tang.Injector;

/**
 * A transient resource policy with fixed parallelism 10 for tests.
 */
public final class TransientResourcePolicyParallelismTen implements Policy {
  private final Policy policy;

  public TransientResourcePolicyParallelismTen() {
    this.policy = new PolicyImpl(
        PolicyTestUtil.overwriteParallelism(10,
            TransientResourcePolicy.BUILDER.getCompileTimePasses()),
        TransientResourcePolicy.BUILDER.getRuntimePasses());
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
