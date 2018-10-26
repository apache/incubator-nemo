package org.apache.nemo.examples.beam.policy;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.UpfrontCloningPass;
import org.apache.nemo.compiler.optimizer.policy.DefaultPolicy;
import org.apache.nemo.compiler.optimizer.policy.Policy;
import org.apache.nemo.compiler.optimizer.policy.PolicyImpl;
import org.apache.reef.tang.Injector;
import java.util.List;

/**
 * A default policy with upfront cloning.
 */
public final class UpfrontSchedulingPolicyParallelismFive implements Policy {
  private final Policy policy;
  public UpfrontSchedulingPolicyParallelismFive() {
    final List<CompileTimePass> overwritingPasses = DefaultPolicy.BUILDER.getCompileTimePasses();
    overwritingPasses.add(new UpfrontCloningPass()); // CLONING!
    this.policy = new PolicyImpl(
        PolicyTestUtil.overwriteParallelism(5, overwritingPasses),
        DefaultPolicy.BUILDER.getRuntimePasses());
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
