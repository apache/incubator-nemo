package org.apache.nemo.compiler.optimizer.pass.compiletime.composite;

import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.TransientResourceDataFlowPass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.TransientResourceDataStorePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.TransientResourcePriorityPass;

import java.util.Arrays;

/**
 * A series of passes to harness transient resources.
 * Ref: https://dl.acm.org/citation.cfm?id=3064181
 */
public final class TransientResourceCompositePass extends CompositePass {
  /**
   * Default constructor.
   */
  public TransientResourceCompositePass() {
    super(Arrays.asList(
        new TransientResourcePriorityPass(),
        new TransientResourceDataStorePass(),
        new TransientResourceDataFlowPass()
    ));
  }
}
