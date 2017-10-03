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
package edu.snu.vortex.compiler.optimizer;

import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.MetricCollectionBarrierVertex;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.optimizer.pass.compiletime.CompileTimePass;
import edu.snu.vortex.compiler.optimizer.pass.runtime.DataSkewRuntimePass;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.optimizer.pass.runtime.RuntimePass;
import edu.snu.vortex.compiler.optimizer.policy.Policy;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;

import java.util.*;

/**
 * Optimizer class.
 */
public final class Optimizer {
  // Private constructor
  private Optimizer() {
  }

  /**
   * Optimize function.
   * @param dag input DAG.
   * @param optimizationPolicy the optimization policy that we want to use to optimize the DAG.
   * @param dagDirectory directory to save the DAG information.
   * @return optimized DAG, tagged with execution properties.
   * @throws Exception throws an exception if there is an exception.
   */
  public static DAG<IRVertex, IREdge> optimize(final DAG<IRVertex, IREdge> dag, final Policy optimizationPolicy,
                                               final String dagDirectory) throws Exception {
    if (optimizationPolicy == null || optimizationPolicy.getCompileTimePasses().isEmpty()) {
      throw new RuntimeException("A policy name should be specified.");
    }
    return process(dag, optimizationPolicy.getCompileTimePasses().iterator(), dagDirectory);
  }

  /**
   * A recursive method to process each pass one-by-one to the given DAG.
   * @param dag DAG to process.
   * @param passes passes to apply.
   * @param dagDirectory directory to save the DAG information.
   * @return the processed DAG.
   * @throws Exception Exceptions on the way.
   */
  private static DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag,
                                               final Iterator<CompileTimePass> passes,
                                               final String dagDirectory) throws Exception {
    if (passes.hasNext()) {
      final CompileTimePass passToApply = passes.next();
      final DAG<IRVertex, IREdge> processedDAG = passToApply.apply(dag);
      processedDAG.storeJSON(dagDirectory, "ir-after-" + passToApply.getClass().getSimpleName(),
          "DAG after optimization");
      return process(processedDAG, passes, dagDirectory);
    } else {
      return dag;
    }
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
    final Class<? extends RuntimePass> dynamicOptimizationType =
        (Class) metricCollectionBarrierVertex.getProperty(ExecutionProperty.Key.DynamicOptimizationType);

    switch (dynamicOptimizationType.getSimpleName()) {
      case DataSkewRuntimePass.SIMPLE_NAME:
        // Map between a partition ID to corresponding metric data (e.g., the size of each block).
        final Map<String, List<Long>> metricData = metricCollectionBarrierVertex.getMetricData();
        return new DataSkewRuntimePass().apply(originalPlan, metricData);
      default:
        return originalPlan;
    }
  }
}
