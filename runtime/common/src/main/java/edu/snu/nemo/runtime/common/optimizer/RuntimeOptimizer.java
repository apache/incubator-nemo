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
package edu.snu.nemo.runtime.common.optimizer;

import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.MetricCollectionBarrierVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.DynamicOptimizationProperty;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.optimizer.pass.runtime.DataSkewRuntimePass;
import edu.snu.nemo.runtime.common.plan.PhysicalPlan;
import edu.snu.nemo.runtime.common.plan.PhysicalPlanGenerator;
import edu.snu.nemo.runtime.common.plan.Stage;
import edu.snu.nemo.runtime.common.plan.StageEdge;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

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
        metricCollectionBarrierVertex.getPropertyValue(DynamicOptimizationProperty.class).get();

    try {
      final PhysicalPlanGenerator physicalPlanGenerator =
          Tang.Factory.getTang().newInjector().getInstance(PhysicalPlanGenerator.class);

      switch (dynamicOptimizationType) {
        case DataSkewRuntimePass:
          // Metric data for DataSkewRuntimePass is a pair of blockIds and map of hashrange, partition size.
          final Pair<List<String>, Map<Integer, Long>> metricData =
              Pair.of(metricCollectionBarrierVertex.getBlockIds(),
                  (Map<Integer, Long>) metricCollectionBarrierVertex.getMetricData());
          final DAG<IRVertex, IREdge> newIrDAG = new DataSkewRuntimePass().apply(originalPlan.getIrDAG(), metricData);
          final DAG<Stage, StageEdge> stageDAG = newIrDAG.convert(physicalPlanGenerator);
          final PhysicalPlan physicalPlan =
              new PhysicalPlan(RuntimeIdGenerator.generatePhysicalPlanId(), newIrDAG, stageDAG);
          return physicalPlan;
        default:
          throw new UnsupportedOperationException("Unknown runtime pass: " + dynamicOptimizationType);
      }
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }
}
