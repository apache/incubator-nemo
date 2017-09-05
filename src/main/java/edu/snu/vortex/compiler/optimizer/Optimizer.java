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
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.compiler.optimizer.passes.*;
import edu.snu.vortex.compiler.optimizer.passes.dynamic_optimization.DataSkewDynamicOptimizationPass;
import edu.snu.vortex.compiler.optimizer.passes.optimization.LoopOptimizations;
import edu.snu.vortex.common.dag.DAG;
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
   * @param policyType type of the instantiation policy that we want to use to optimize the DAG.
   * @param dagDirectory directory to save the DAG information.
   * @return optimized DAG, tagged with attributes.
   * @throws Exception throws an exception if there is an exception.
   */
  public static DAG<IRVertex, IREdge> optimize(final DAG<IRVertex, IREdge> dag, final PolicyType policyType,
                                               final String dagDirectory) throws Exception {
    if (policyType == null) {
      throw new RuntimeException("Policy has not been provided for the policyType");
    }
    return process(dag, POLICIES.get(policyType), dagDirectory);
  }

  /**
   * A recursive method to process each pass one-by-one to the given DAG.
   * @param dag DAG to process.
   * @param passes passes to apply.
   * @param dagDirectory directory to save the DAG information.
   * @return the processed DAG.
   * @throws Exception Exceptionso n the way.
   */
  private static DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag,
                                               final List<StaticOptimizationPass> passes,
                                               final String dagDirectory) throws Exception {
    if (passes.isEmpty()) {
      return dag;
    } else {
      final DAG<IRVertex, IREdge> processedDAG = passes.get(0).process(dag);
      processedDAG.storeJSON(dagDirectory, "ir-after-" + passes.get(0).getClass().getSimpleName(),
          "DAG after optimization");
      return process(processedDAG, passes.subList(1, passes.size()), dagDirectory);
    }
  }

  /**
   * Enum for different types of instantiation policies.
   */
  public enum PolicyType {
    Default,
    Pado,
    Disaggregation,
    DataSkew,
    TestingPolicy,
  }

  /**
   * A HashMap to match each of instantiation policies with a combination of instantiation passes.
   * Each policies are run in the order with which they are defined.
   */
  private static final Map<PolicyType, List<StaticOptimizationPass>> POLICIES = new HashMap<>();
  static {
    POLICIES.put(PolicyType.Default,
        Arrays.asList(
            new ParallelismPass(), // Provides parallelism information.
            new DefaultStagePartitioningPass(),
            new ScheduleGroupPass()
        ));
    POLICIES.put(PolicyType.Pado,
        Arrays.asList(
            new ParallelismPass(), // Provides parallelism information.
            new LoopGroupingPass(),
            LoopOptimizations.getLoopFusionPass(),
            LoopOptimizations.getLoopInvariantCodeMotionPass(),
            new LoopUnrollingPass(), // Groups then unrolls loops. TODO #162: remove unrolling pt.
            new PadoVertexPass(), new PadoEdgePass(), // Processes vertices and edges with Pado algorithm.
            new DefaultStagePartitioningPass(),
            new ScheduleGroupPass()
       ));
    POLICIES.put(PolicyType.Disaggregation,
        Arrays.asList(
            new ParallelismPass(), // Provides parallelism information.
            new LoopGroupingPass(),
            LoopOptimizations.getLoopFusionPass(),
            LoopOptimizations.getLoopInvariantCodeMotionPass(),
            new LoopUnrollingPass(), // Groups then unrolls loops. TODO #162: remove unrolling pt.
            new DisaggregationPass(), // Processes vertices and edges with Disaggregation algorithm.
            new IFilePass(), // Enables I-File style write optimization.
            new DefaultStagePartitioningPass(),
            new ScheduleGroupPass()
        ));
    POLICIES.put(PolicyType.DataSkew,
        Arrays.asList(
            new ParallelismPass(), // Provides parallelism information.
            new LoopGroupingPass(),
            LoopOptimizations.getLoopFusionPass(),
            LoopOptimizations.getLoopInvariantCodeMotionPass(),
            new LoopUnrollingPass(), // Groups then unrolls loops. TODO #162: remove unrolling pt.
            new DataSkewPass(),
            new DefaultStagePartitioningPass(),
            new ScheduleGroupPass()
        ));
    POLICIES.put(PolicyType.TestingPolicy, // Simply build stages for tests
        Arrays.asList(
            new DefaultStagePartitioningPass(),
            new ScheduleGroupPass()
        ));
  }

  /**
   * A HashMap to convert string names for each policy type to receive as arguments.
   */
  public static final Map<String, PolicyType> POLICY_NAME = new HashMap<>();
  static {
    POLICY_NAME.put("default", PolicyType.Default);
    POLICY_NAME.put("pado", PolicyType.Pado);
    POLICY_NAME.put("disaggregation", PolicyType.Disaggregation);
    POLICY_NAME.put("dataskew", PolicyType.DataSkew);
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
    final Attribute dynamicOptimizationType =
        metricCollectionBarrierVertex.getAttr(Attribute.Key.DynamicOptimizationType);

    switch (dynamicOptimizationType) {
      case DataSkew:
        // Map between a partition ID to corresponding metric data (e.g., the size of each block).
        final Map<String, List<Long>> metricData = metricCollectionBarrierVertex.getMetricData();
        return new DataSkewDynamicOptimizationPass().process(originalPlan, metricData);
      default:
        return originalPlan;
    }
  }
}
