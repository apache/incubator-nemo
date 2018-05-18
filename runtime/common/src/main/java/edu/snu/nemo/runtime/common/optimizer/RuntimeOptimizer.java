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
package edu.snu.nemo.runtime.common.optimizer;

import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.ir.vertex.MetricCollectionBarrierVertex;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.DynamicOptimizationProperty;
import edu.snu.nemo.runtime.common.optimizer.pass.runtime.DataSkewRuntimePass;
import edu.snu.nemo.runtime.common.plan.physical.PhysicalPlan;

import java.util.*;

/**
 * Runtime optimizer class.
 */
public final class RuntimeOptimizer {
  /**
   * Private constructor.
   */
  private RuntimeOptimizer() {
  }

  /**
   * Dynamic optimization method to process the dag with an appropriate pass, decided by the stats.
   * @param originalPlan original physical execution plan.
   * @param metricCollectionBarrierVertex the vertex that collects metrics and chooses which optimization to perform.
   * @return the newly updated optimized physical plan.
   */
  public static synchronized PhysicalPlan dynamicOptimization(
          final PhysicalPlan originalPlan,
          final MetricCollectionBarrierVertex metricCollectionBarrierVertex) {
    final DynamicOptimizationProperty.Value dynamicOptimizationType =
        metricCollectionBarrierVertex.getProperty(ExecutionProperty.Key.DynamicOptimizationType);

    switch (dynamicOptimizationType) {
      case DataSkewRuntimePass:
        // Map between a partition ID to corresponding metric data (e.g., the size of each block).
        final Map<String, List<Pair<Integer, Long>>> metricData = metricCollectionBarrierVertex.getMetricData();
        return new DataSkewRuntimePass().apply(originalPlan, metricData);
      default:
        throw new UnsupportedOperationException("Unknown runtime pass: " + dynamicOptimizationType);
    }
  }
}
