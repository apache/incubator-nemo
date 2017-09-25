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
package edu.snu.vortex.compiler.optimizer.pass.dynamic_optimization;

import com.google.common.annotations.VisibleForTesting;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.common.dag.DAGBuilder;
import edu.snu.vortex.compiler.exception.DynamicOptimizationException;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalStage;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalStageEdge;
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.executor.data.HashRange;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Dynamic optimization pass for handling data skew.
 */
public final class DataSkewDynamicOptimizationPass implements DynamicOptimizationPass<Long> {
  public static final String SIMPLE_NAME = "DataSkewDynamicOptimizationPass";

  @Override
  public PhysicalPlan apply(final PhysicalPlan originalPlan, final Map<String, List<Long>> metricData) {
    // Builder to create new stages.
    final DAGBuilder<PhysicalStage, PhysicalStageEdge> physicalDAGBuilder =
        new DAGBuilder<>(originalPlan.getStageDAG());

    // get edges to optimize
    final List<String> optimizationEdgeIds = metricData.keySet().stream().map(partitionId ->
        RuntimeIdGenerator.parsePartitionId(partitionId)[0]).collect(Collectors.toList());
    final DAG<PhysicalStage, PhysicalStageEdge> stageDAG = originalPlan.getStageDAG();
    final List<PhysicalStageEdge> optimizationEdges = stageDAG.getVertices().stream()
        .flatMap(physicalStage -> stageDAG.getIncomingEdgesOf(physicalStage).stream())
        .filter(physicalStageEdge -> optimizationEdgeIds.contains(physicalStageEdge.getId()))
        .collect(Collectors.toList());

    // Get number of evaluators of the next stage (number of partitions).
    final Integer taskGroupListSize = optimizationEdges.stream().findFirst().orElseThrow(() ->
        new RuntimeException("optimization edges is empty")).getDst().getTaskGroupList().size();

    // Calculate hashRanges.
    final List<HashRange> hashRanges = calculateHashRanges(metricData, taskGroupListSize);

    // Assign the hash value range to each receiving task group.
    optimizationEdges.forEach(optimizationEdge -> {
      final List<TaskGroup> taskGroups = optimizationEdge.getDst().getTaskGroupList();
      final Map<String, HashRange> taskGroupIdToHashRangeMap = optimizationEdge.getTaskGroupIdToHashRangeMap();
      IntStream.range(0, taskGroupListSize).forEach(i ->
          // Update the information.
          taskGroupIdToHashRangeMap.put(taskGroups.get(i).getTaskGroupId(), hashRanges.get(i)));
    });

    return new PhysicalPlan(originalPlan.getId(), physicalDAGBuilder.build(), originalPlan.getTaskIRVertexMap());
  }

  @VisibleForTesting
  List<HashRange> calculateHashRanges(final Map<String, List<Long>> metricData, final Integer taskGroupListSize) {
    // NOTE: metricData is made up of a map of partitionId to blockSizes.
    // Count the hash range (number of blocks for each partition).
    final int hashRangeCount = metricData.values().stream().findFirst().orElseThrow(() ->
        new DynamicOptimizationException("no valid metric data.")).size();

    // Aggregate metric data. TODO #458: aggregate metric data beforehand.
    final List<Long> aggregatedMetricData = new ArrayList<>(hashRangeCount);
    // for each hash range index, we aggregate the metric data.
    IntStream.range(0, hashRangeCount).forEach(i ->
        aggregatedMetricData.add(i, metricData.values().stream().mapToLong(lst -> lst.get(i)).sum()));

    // Do the optimization using the information derived above.
    final Long totalSize = aggregatedMetricData.stream().mapToLong(n -> n).sum(); // get total size
    final Long idealSizePerTaskGroup = totalSize / taskGroupListSize; // and derive the ideal size per task group

    // find HashRanges to apply (for each blocks of each partition).
    final List<HashRange> hashRanges = new ArrayList<>(taskGroupListSize);
    int startingHashValue = 0;
    int finishingHashValue = 1; // initial values
    Long currentAccumulatedSize = aggregatedMetricData.get(0); // what we have up to now
    for (int i = 1; i <= taskGroupListSize; i++) {
      if (i != taskGroupListSize) {
        final Long idealAccumulatedSize = idealSizePerTaskGroup * i; // where we should end
        // find the point while adding up one by one.
        while (currentAccumulatedSize < idealAccumulatedSize) {
          currentAccumulatedSize += aggregatedMetricData.get(finishingHashValue);
          finishingHashValue++;
        }
        // Go back once if we came too far.
        if (currentAccumulatedSize - idealAccumulatedSize
            > idealAccumulatedSize - (currentAccumulatedSize - aggregatedMetricData.get(finishingHashValue - 1))) {
          finishingHashValue--;
          currentAccumulatedSize -= aggregatedMetricData.get(finishingHashValue);
        }
        // assign appropriately
        hashRanges.add(i - 1, HashRange.of(startingHashValue, finishingHashValue));
        startingHashValue = finishingHashValue;
      } else { // last one: we put the range of the rest.
        hashRanges.add(i - 1, HashRange.of(startingHashValue, hashRangeCount));
      }
    }
    return hashRanges;
  }
}
