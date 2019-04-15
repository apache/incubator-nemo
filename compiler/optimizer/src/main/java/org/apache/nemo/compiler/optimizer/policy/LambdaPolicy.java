package org.apache.nemo.compiler.optimizer.policy;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.PipeTransferForAllEdgesPass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.composite.DefaultCompositePass;
import org.apache.nemo.compiler.optimizer.pass.runtime.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Lambda Policy
 * Maintainer: Gao Zhiyuan
 * Description: A part of lambda executor, to support Lambda pass.
 */
public final class LambdaPolicy implements Policy {
  private final Policy policy;
  private static final Logger LOG = LoggerFactory.getLogger(LambdaPolicy.class.getName());

  /**
   * Default constructor.
   */
  public LambdaPolicy() {
    LOG.info("LambdaPolicy!!!!!");
    final PolicyBuilder builder = new PolicyBuilder();
    builder.registerCompileTimePass(new DefaultCompositePass());
    builder.registerCompileTimePass(new PipeTransferForAllEdgesPass());
    this.policy = builder.build();
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
