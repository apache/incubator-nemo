package org.apache.nemo.compiler.optimizer.policy;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.composite.DefaultCompositePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.composite.LargeShuffleCompositePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.composite.LoopOptimizationCompositePass;
import org.apache.reef.tang.Injector;

/**
 * A policy to demonstrate the large shuffle optimization, witch batches disk seek during data shuffle, conditionally.
 */
public final class ConditionalLargeShufflePolicy implements Policy {
  public static final PolicyBuilder BUILDER =
      new PolicyBuilder()
          .registerCompileTimePass(new LargeShuffleCompositePass(), dag -> getMaxParallelism(dag) > 300)
          .registerCompileTimePass(new LoopOptimizationCompositePass())
          .registerCompileTimePass(new DefaultCompositePass());
  private final Policy policy;

  /**
   * Default constructor.
   */
  public ConditionalLargeShufflePolicy() {
    this.policy = BUILDER.build();
  }

  /**
   * Returns the maximum parallelism of the vertices of a IR DAG.
   * @param dag dag to observe.
   * @return the maximum parallelism, or 1 by default.
   */
  private static int getMaxParallelism(final DAG<IRVertex, IREdge> dag) {
    return dag.getVertices().stream()
        .mapToInt(vertex -> vertex.getPropertyValue(ParallelismProperty.class).orElse(1))
        .max().orElse(1);
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
