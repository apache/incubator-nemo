package org.apache.nemo.compiler.optimizer.policy;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultScheduleGroupPass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.AggressiveSpeculativeCloningPass;
import org.apache.reef.tang.Injector;

/**
 * Basic pull policy.
 * TODO #200: Maintain Test Passes and Policies Separately
 */
public final class BasicPullPolicy implements Policy {
  public static final PolicyBuilder BUILDER =
      new PolicyBuilder()
          .registerCompileTimePass(new AggressiveSpeculativeCloningPass())
          .registerCompileTimePass(new DefaultScheduleGroupPass());
  private final Policy policy;

  /**
   * Default constructor.
   */
  public BasicPullPolicy() {
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
