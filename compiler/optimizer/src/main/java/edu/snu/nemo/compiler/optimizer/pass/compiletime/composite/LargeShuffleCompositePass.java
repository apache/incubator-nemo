/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.compiler.optimizer.pass.compiletime.composite;

import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.*;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.reshaping.LargeShuffleRelayReshapingPass;

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
        new LargeSuffleCompressionPass(),
        new LargeShuffleDecompressionPass(),
        new LargeShuffleDataPersistencePass(),
        new LargeShuffleResourceSlotPass()
    ));
  }
}
