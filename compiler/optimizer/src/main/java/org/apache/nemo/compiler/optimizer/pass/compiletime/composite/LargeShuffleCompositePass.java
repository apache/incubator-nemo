package org.apache.nemo.compiler.optimizer.pass.compiletime.composite;

import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.*;
import org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping.LargeShuffleRelayReshapingPass;

import java.util.Arrays;

/**
 * A series of passes to optimize large shuffle with disk seek batching techniques.
 * Ref. https://dl.acm.org/citation.cfm?id=2391233
 */
public final class LargeShuffleCompositePass extends CompositePass {
  /**
   * Default constructor.
   */
  public LargeShuffleCompositePass() {
    super(Arrays.asList(
        new LargeShuffleRelayReshapingPass(),
        new LargeShuffleDataFlowPass(),
        new LargeShuffleDataStorePass(),
        new LargeShuffleDecoderPass(),
        new LargeShuffleEncoderPass(),
        new LargeShufflePartitionerPass(),
        new LargeShuffleCompressionPass(),
        new LargeShuffleDecompressionPass(),
        new LargeShuffleDataPersistencePass(),
        new LargeShuffleResourceSlotPass()
    ));
  }
}
