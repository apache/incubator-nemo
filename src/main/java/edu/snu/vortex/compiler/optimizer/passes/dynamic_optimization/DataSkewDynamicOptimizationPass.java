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
package edu.snu.vortex.compiler.optimizer.passes.dynamic_optimization;

import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.common.dag.DAGBuilder;
import edu.snu.vortex.compiler.exception.DynamicOptimizationException;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalStage;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalStageEdge;
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.executor.data.HashRange;

import java.util.List;
import java.util.Map;

/**
 * Dynamic optimization pass for handling data skew.
 */
public final class DataSkewDynamicOptimizationPass implements DynamicOptimizationPass {
  @Override
  public PhysicalPlan process(final PhysicalPlan originalPlan, final Map<String, List> metricData) {
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
      final Map<String, HashRange> taskGroupIdToHashRangeMap = optimizationEdge.getTaskGroupIdToHashRangeMap();
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
  }
}
