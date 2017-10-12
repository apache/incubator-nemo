/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.vortex.compiler.optimizer.pass.compiletime.composite;

import edu.snu.vortex.compiler.optimizer.pass.compiletime.annotating.*;

import java.util.Arrays;

/**
 * A series of passes to support Disaggregated Resources.
 */
public final class DisaggregationPass extends CompositePass {
  public static final String SIMPLE_NAME = "DisaggregationPass";

  public DisaggregationPass() {
    super(Arrays.asList(
        new DisaggregationVertexPass(),
        new DisaggregationEdgeDataStorePass(),
        new DisaggregationEdgeDataFlowModelPass(),
        new DisaggregationEdgePartitionerPass(),
        new IFilePass()
    ));
  }

  @Override
  public String getName() {
    return SIMPLE_NAME;
  }
}
