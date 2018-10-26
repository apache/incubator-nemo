package org.apache.nemo.compiler.optimizer.pass.compiletime.composite;

import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.*;

import java.util.Arrays;

/**
 * A series of primitive passes that is applied commonly to all policies.
 * It is highly recommended to place reshaping passes before this pass,
 * and annotating passes after that and before this pass.
 */
public final class DefaultCompositePass extends CompositePass {
  /**
   * Default constructor.
   */
  public DefaultCompositePass() {
    super(Arrays.asList(
        new DefaultParallelismPass(),
        new DefaultMetricPass(),
        new DefaultEdgeEncoderPass(),
        new DefaultEdgeDecoderPass(),
        new DefaultDataStorePass(),
        new DefaultDataPersistencePass(),
        new DefaultScheduleGroupPass(),
        new CompressionPass(),
        new DecompressionPass(),
        new ResourceLocalityPass(),
        new ResourceSitePass(),
        new ResourceSlotPass()
    ));
  }
}
