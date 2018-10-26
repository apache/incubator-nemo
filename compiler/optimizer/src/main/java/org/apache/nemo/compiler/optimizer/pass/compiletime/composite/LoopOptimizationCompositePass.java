package org.apache.nemo.compiler.optimizer.pass.compiletime.composite;

import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.DuplicateEdgeGroupSizePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping.LoopExtractionPass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping.LoopOptimizations;
import org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping.LoopUnrollingPass;

import java.util.Arrays;

/**
 * A series of passes to perform LoopOptimization.
 */
public final class LoopOptimizationCompositePass extends CompositePass {
  /**
   * Default constructor.
   */
  public LoopOptimizationCompositePass() {
    super(Arrays.asList(
        new LoopExtractionPass(),
        LoopOptimizations.getLoopFusionPass(),
        LoopOptimizations.getLoopInvariantCodeMotionPass(),
        new LoopUnrollingPass(), // Groups then unrolls loops.
        new DuplicateEdgeGroupSizePass()
    ));
  }
}
