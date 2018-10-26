package org.apache.nemo.compiler.optimizer.policy;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultParallelismPass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultDataStorePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultScheduleGroupPass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.composite.CompositePass;
import org.apache.reef.tang.Injector;

import java.util.Arrays;

/**
 * A simple example policy to demonstrate a policy with a separate, refactored pass.
 * It simply performs what is done with the default pass.
 * This example simply shows that users can define their own pass in their policy.
 */
public final class DefaultPolicyWithSeparatePass implements Policy {
  public static final PolicyBuilder BUILDER =
      new PolicyBuilder()
          .registerCompileTimePass(new DefaultParallelismPass())
          .registerCompileTimePass(new RefactoredPass());
  private final Policy policy;

  /**
   * Default constructor.
   */
  public DefaultPolicyWithSeparatePass() {
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

  /**
   * A simple custom pass consisted of the two passes at the end of the default pass.
   */
  public static final class RefactoredPass extends CompositePass {
    /**
     * Default constructor.
     */
    RefactoredPass() {
      super(Arrays.asList(
          new DefaultDataStorePass(),
          new DefaultScheduleGroupPass()
      ));
    }
  }
}
