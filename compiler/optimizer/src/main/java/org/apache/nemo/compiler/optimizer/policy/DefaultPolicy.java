package org.apache.nemo.compiler.optimizer.policy;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.composite.DefaultCompositePass;
import org.apache.reef.tang.Injector;

/**
 * A basic default policy, that performs the minimum amount of optimization to be done to a specific DAG.
 */
public final class DefaultPolicy implements Policy {
  public static final PolicyBuilder BUILDER =
      new PolicyBuilder()
          .registerCompileTimePass(new DefaultCompositePass());
  private final Policy policy;

  /**
   * Default constructor.
   */
  public DefaultPolicy() {
    this.policy = BUILDER.build();
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
