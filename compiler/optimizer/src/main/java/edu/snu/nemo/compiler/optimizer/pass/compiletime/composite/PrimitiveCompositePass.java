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

import java.util.Arrays;

/**
 * A series of primitive passes that is applied commonly to all policies.
 * It is highly recommended to place reshaping passes before this pass,
 * and annotating passes after that and before this pass.
 */
public final class PrimitiveCompositePass extends CompositePass {
  /**
   * Default constructor.
   */
  public PrimitiveCompositePass() {
    super(Arrays.asList(
        new DefaultParallelismPass(),
        new DefaultMetricPass(),
        new DefaultEdgeEncoderPass(),
        new DefaultEdgeDecoderPass(),
        new DefaultInterTaskDataStorePass(),
        new DefaultEdgeUsedDataHandlingPass(),
        new DefaultScheduleGroupPass(),
        new CompressionPass(),
        new DecompressionPass(),
        new SourceLocationAwareSchedulingPass(),
        new ExecutorSlotCompliancePass()
    ));
  }
}
