package org.apache.nemo.compiler.optimizer.policy;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.*;
import org.apache.nemo.compiler.optimizer.pass.compiletime.composite.DefaultCompositePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.composite.LoopOptimizationCompositePass;
import org.apache.reef.tang.Injector;

/**
 * A policy to demonstrate the disaggregation optimization, that uses GlusterFS as file  storage.
 */
public final class DisaggregationPolicy implements Policy {
  public static final PolicyBuilder BUILDER =
      new PolicyBuilder()
          .registerCompileTimePass(new DisaggregationEdgeDataStorePass())
          .registerCompileTimePass(new LoopOptimizationCompositePass())
          .registerCompileTimePass(new DefaultCompositePass());
  private final Policy policy;

  /**
   * Default constructor.
   */
  public DisaggregationPolicy() {
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
