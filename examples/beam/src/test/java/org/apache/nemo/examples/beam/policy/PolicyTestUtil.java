package org.apache.nemo.examples.beam.policy;

import org.apache.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.DefaultParallelismPass;

import java.util.List;

/**
 * Test util for testing policies.
 */
public final class PolicyTestUtil {
  /**
   * Overwrite the parallelism of existing policy.
   *
   * @param desiredSourceParallelism       the desired source parallelism to set.
   * @param compileTimePassesToOverwrite   the list of compile time passes to overwrite.
   * @return the overwritten policy.
   */
  public static List<CompileTimePass> overwriteParallelism(final int desiredSourceParallelism,
                                            final List<CompileTimePass> compileTimePassesToOverwrite) {
    final int parallelismPassIdx = compileTimePassesToOverwrite.indexOf(new DefaultParallelismPass());
    compileTimePassesToOverwrite.set(parallelismPassIdx,
        new DefaultParallelismPass(desiredSourceParallelism, 2));
    return compileTimePassesToOverwrite;
  }
}
