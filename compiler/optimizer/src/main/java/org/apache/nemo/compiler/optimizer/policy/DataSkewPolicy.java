package org.apache.nemo.compiler.optimizer.policy;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.composite.SkewCompositePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.composite.LoopOptimizationCompositePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.composite.DefaultCompositePass;
import org.apache.nemo.runtime.common.optimizer.pass.runtime.DataSkewRuntimePass;
import org.apache.reef.tang.Injector;

/**
 * A policy to perform data skew dynamic optimization.
 */
public final class DataSkewPolicy implements Policy {
  public static final PolicyBuilder BUILDER =
      new PolicyBuilder()
        .registerRuntimePass(new DataSkewRuntimePass().setNumSkewedKeys(DataSkewRuntimePass.DEFAULT_NUM_SKEWED_KEYS),
          new SkewCompositePass())
        .registerCompileTimePass(new LoopOptimizationCompositePass())
        .registerCompileTimePass(new DefaultCompositePass());

  private final Policy policy;

  /**
   * Default constructor.
   */
  public DataSkewPolicy() {
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
