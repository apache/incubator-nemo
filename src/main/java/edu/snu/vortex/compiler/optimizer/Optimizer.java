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

import edu.snu.vortex.common.dag.DAGBuilder;
import edu.snu.vortex.compiler.exception.DynamicOptimizationException;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.MetricCollectionBarrierVertex;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.compiler.optimizer.passes.*;
import edu.snu.vortex.compiler.optimizer.passes.optimization.LoopOptimizations;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalStage;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalStageEdge;
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.executor.data.HashRange;

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
  private static DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag, final List<Pass> passes,
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
  private static final Map<PolicyType, List<Pass>> POLICIES = new HashMap<>();
  static {
    POLICIES.put(PolicyType.Default,
        Arrays.asList(
            new ParallelismPass(), // Provides parallelism information.
            new DefaultStagePartitioningPass()
        ));
    POLICIES.put(PolicyType.Pado,
        Arrays.asList(
            new ParallelismPass(), // Provides parallelism information.
            new LoopGroupingPass(),
            LoopOptimizations.getLoopFusionPass(),
            LoopOptimizations.getLoopInvariantCodeMotionPass(),
            new LoopUnrollingPass(), // Groups then unrolls loops. TODO #162: remove unrolling pt.
            new PadoVertexPass(), new PadoEdgePass(), // Processes vertices and edges with Pado algorithm.
            new DefaultStagePartitioningPass()
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
            new DefaultStagePartitioningPass()
        ));
    POLICIES.put(PolicyType.DataSkew,
        Arrays.asList(
            new ParallelismPass(), // Provides parallelism information.
            new LoopGroupingPass(),
            LoopOptimizations.getLoopFusionPass(),
            LoopOptimizations.getLoopInvariantCodeMotionPass(),
            new LoopUnrollingPass(), // Groups then unrolls loops. TODO #162: remove unrolling pt.
            new DataSkewPass(),
            new DefaultStagePartitioningPass()
        ));
    POLICIES.put(PolicyType.TestingPolicy, // Simply build stages for tests
            Arrays.asList(
                    new DefaultStagePartitioningPass()
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
   * @return processed DAG.
   */
  public static PhysicalPlan dynamicOptimization(final PhysicalPlan originalPlan,
                                                 final MetricCollectionBarrierVertex metricCollectionBarrierVertex) {
    // Map between a partition ID to corresponding metric data (e.g., the size of each block).
    final Map<String, List> metricData = metricCollectionBarrierVertex.getMetricData();
    final Attribute dynamicOptimizationType =
        metricCollectionBarrierVertex.getAttr(Attribute.Key.DynamicOptimizationType);

    switch (dynamicOptimizationType) {
      case DataSkew:

        // Builder to create new stages.
        final DAGBuilder<PhysicalStage, PhysicalStageEdge> physicalDAGBuilder =
            new DAGBuilder<>(originalPlan.getStageDAG());

        // Count the hash range.
        final int hashRange = metricData.values().stream().findFirst().orElseThrow(() ->
            new DynamicOptimizationException("no valid metric data.")).size();

        // Do the optimization using the information derived above.
        metricData.forEach((partitionId, partitionSizes) -> {
          final String runtimeEdgeId = RuntimeIdGenerator.parsePartitionId(partitionId)[0];
          final DAG<PhysicalStage, PhysicalStageEdge> stageDAG = originalPlan.getStageDAG();
          // Edge of the partition.
          final PhysicalStageEdge optimizationEdge = stageDAG.getVertices().stream()
              .flatMap(physicalStage -> stageDAG.getIncomingEdgesOf(physicalStage).stream())
              .filter(physicalStageEdge -> physicalStageEdge.getId().equals(runtimeEdgeId))
              .findFirst().orElseThrow(() ->
                  new DynamicOptimizationException("physical stage DAG doesn't contain this edge: " + runtimeEdgeId));
          // The following stage to receive the data.
          final PhysicalStage optimizationStage = optimizationEdge.getDst();

          // Assign the hash value range to each receiving task group.
          // TODO #390: DynOpt-Update data skew handling policy
          final List<TaskGroup> taskGroups = optimizationEdge.getDst().getTaskGroupList();
          final Map<String, HashRange> taskGroupIdToHashRangeMap =
              optimizationEdge.getTaskGroupIdToHashRangeMap();
          final int quotient = hashRange / taskGroups.size();
          final int remainder = hashRange % taskGroups.size();
          int assignedHashValue = 0;
          for (int i = 0; i < taskGroups.size(); i++) {
            final TaskGroup taskGroup = taskGroups.get(i);
            final HashRange hashRangeToAssign;
            if (i == taskGroups.size() - 1) {
              // last one.
              hashRangeToAssign = HashRange.of(assignedHashValue, assignedHashValue + quotient + remainder);
            } else {
              hashRangeToAssign = HashRange.of(assignedHashValue, assignedHashValue + quotient);
            }
            assignedHashValue += quotient;
            taskGroupIdToHashRangeMap.put(taskGroup.getTaskGroupId(), hashRangeToAssign);
          }
        });

        return new PhysicalPlan(originalPlan.getId(), physicalDAGBuilder.build(), originalPlan.getTaskIRVertexMap());
      default:
        return originalPlan;
    }
  }
}
