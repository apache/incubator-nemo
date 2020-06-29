package org.apache.nemo.compiler.optimizer.policy;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.*;
import org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping.LoopUnrollingPass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping.SamplingTaskSizingPass;
import org.apache.nemo.compiler.optimizer.pass.runtime.Message;

/**
 *
 */
public class DynamicTaskSizingPolicy implements Policy {
    public static final PolicyBuilder BUILDER =
      new PolicyBuilder()
        .registerCompileTimePass(new DefaultParallelismPass())
        .registerCompileTimePass(new SamplingTaskSizingPass())
        .registerCompileTimePass(new LoopUnrollingPass())
        .registerCompileTimePass(new DefaultEdgeEncoderPass())
        .registerCompileTimePass(new DefaultEdgeDecoderPass())
        .registerCompileTimePass(new DefaultDataStorePass())
        .registerCompileTimePass(new DefaultDataPersistencePass())
        .registerCompileTimePass(new DefaultScheduleGroupPass())
        .registerCompileTimePass(new CompressionPass())
        .registerCompileTimePass(new ResourceLocalityPass())
        .registerCompileTimePass(new ResourceSlotPass());
    private final Policy policy;

    /**
     * Default constructor.
     */
    public DynamicTaskSizingPolicy() {
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
