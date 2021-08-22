package org.apache.nemo.compiler.optimizer.policy;

import com.sun.corba.se.spi.orbutil.threadpool.Work;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.vertex.executionproperty.EnableWorkStealingExecutionProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.WorkStealingPass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.composite.DefaultCompositePass;
import org.apache.nemo.compiler.optimizer.pass.runtime.Message;

public class WorkStealingPolicy implements Policy {
  public static final PolicyBuilder BUILDER =
    new PolicyBuilder()
      .registerCompileTimePass(new DefaultCompositePass())
      .registerCompileTimePass(new WorkStealingPass());

  private final Policy policy;

  /**
   * Default constructor.
   */
  public WorkStealingPolicy() {
    this.policy = BUILDER.build();
  }

  @Override
  public IRDAG runCompileTimeOptimization(final IRDAG dag, final String dagDirectory) {
    return this.policy.runCompileTimeOptimization(dag, dagDirectory);
  }

  @Override
  public IRDAG runRunTimeOptimizations(final IRDAG dag, final Message<?> message) {
    return this.policy.runRunTimeOptimizations(dag, message);
  }
}
