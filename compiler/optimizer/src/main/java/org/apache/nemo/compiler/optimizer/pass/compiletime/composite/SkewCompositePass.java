package org.apache.nemo.compiler.optimizer.pass.compiletime.composite;

import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.*;
import org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping.SkewReshapingPass;

import java.util.Arrays;

/**
 * Pass to modify the DAG for a job to perform data skew.
 */
public final class SkewCompositePass extends CompositePass {
  /**
   * Default constructor.
   */
  public SkewCompositePass() {
    super(Arrays.asList(
        new SkewReshapingPass(),
        new SkewResourceSkewedDataPass(),
        new SkewMetricCollectionPass()
    ));
  }
}
