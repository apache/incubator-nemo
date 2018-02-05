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
package edu.snu.coral.runtime.common.optimizer.pass.runtime;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.snu.coral.common.eventhandler.CommonEventHandler;
import edu.snu.coral.common.dag.DAG;
import edu.snu.coral.common.dag.DAGBuilder;
import edu.snu.coral.common.exception.DynamicOptimizationException;

import edu.snu.coral.runtime.common.RuntimeIdGenerator;
import edu.snu.coral.runtime.common.data.KeyRange;
import edu.snu.coral.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.coral.runtime.common.plan.physical.PhysicalStage;
import edu.snu.coral.runtime.common.plan.physical.PhysicalStageEdge;
import edu.snu.coral.runtime.common.data.HashRange;
import edu.snu.coral.runtime.common.eventhandler.DynamicOptimizationEventHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Dynamic optimization pass for handling data skew.
 */
public final class DataSkewRuntimePass implements RuntimePass<Map<String, List<Long>>> {
  private final Set<Class<? extends CommonEventHandler<?>>> eventHandlers;
  private static final Logger LOG = LoggerFactory.getLogger(DataSkewRuntimePass.class.getName());

  /**
   * Constructor.
   */
  public DataSkewRuntimePass() {
    this.eventHandlers = Stream.of(
        DynamicOptimizationEventHandler.class
    ).collect(Collectors.toSet());
  }

  @Override
  public Set<Class<? extends CommonEventHandler<?>>> getEventHandlers() {
    return eventHandlers;
  }

  @Override
  public PhysicalPlan apply(final PhysicalPlan originalPlan, final Map<String, List<Long>> metricData) {
    // Builder to create new stages.
    final DAGBuilder<PhysicalStage, PhysicalStageEdge> physicalDAGBuilder =
        new DAGBuilder<>(originalPlan.getStageDAG());

    // get edges to optimize
    final List<String> optimizationEdgeIds = metricData.keySet().stream().map(blockId ->
        RuntimeIdGenerator.getRuntimeEdgeIdFromBlockId(blockId)).collect(Collectors.toList());
    final DAG<PhysicalStage, PhysicalStageEdge> stageDAG = originalPlan.getStageDAG();
    final List<PhysicalStageEdge> optimizationEdges = stageDAG.getVertices().stream()
        .flatMap(physicalStage -> stageDAG.getIncomingEdgesOf(physicalStage).stream())
        .filter(physicalStageEdge -> optimizationEdgeIds.contains(physicalStageEdge.getId()))
        .collect(Collectors.toList());

    for (PhysicalStageEdge pEdge : optimizationEdges) {
      LOG.info("Skew: optimizationEdge {} from {}", pEdge.getId(), pEdge.getSrcVertex());
    }
    // Get number of evaluators of the next stage (number of blocks).
    final Integer taskGroupListSize = optimizationEdges.stream().findFirst().orElseThrow(() ->
        new RuntimeException("optimization edges are empty")).getDst().getTaskGroupIds().size();
    LOG.info("Skew: taskGroupListSize {}", taskGroupListSize);

    // Calculate keyRanges.
    final List<KeyRange> keyRanges = calculateHashRanges(metricData, taskGroupListSize);
    LOG.info("Skew: calculated key ranges {}", keyRanges);

    // Overwrite the previously assigned hash value range in the physical DAG with the new range.
    optimizationEdges.forEach(optimizationEdge -> {
      // Update the information.
      final List<KeyRange> taskGroupIdxToHashRange = new ArrayList<>();
      IntStream.range(0, taskGroupListSize).forEach(i -> taskGroupIdxToHashRange.add(keyRanges.get(i)));
      optimizationEdge.setTaskGroupIdxToKeyRange(taskGroupIdxToHashRange);
      LOG.info("Skew: optimizationEdge {} from {}: keyRange {}", optimizationEdge.getId(),
          optimizationEdge.getSrcVertex(), taskGroupIdxToHashRange);
    });

    return new PhysicalPlan(originalPlan.getId(), physicalDAGBuilder.build(), originalPlan.getTaskIRVertexMap());
  }

  /**
   * Method for calculating key ranges to evenly distribute the skewed metric data.
   * @param metricData the metric data.
   * @param taskGroupListSize the size of the task group list.
   * @return  the list of key ranges calculated.
   */
  @VisibleForTesting
  public List<KeyRange> calculateHashRanges(final Map<String, List<Long>> metricData,
                                            final Integer taskGroupListSize) {
    // NOTE: metricData is made up of a map of blockId to blockSizes.
    // Count the hash range (number of blocks for each block).
    final int hashRangeCount = metricData.values().stream().findFirst().orElseThrow(() ->
        new DynamicOptimizationException("no valid metric data.")).size();
    LOG.info("Skew: hashRangeCount from metric {}", hashRangeCount);
    for (Map.Entry<String, List<Long>> metric : metricData.entrySet()) {
      LOG.info("Skew: metric: blockId {} blockSizes {}", metric.getKey(), metric.getValue());
    }
    // Aggregate metric data.
    final List<Long> aggregatedMetricData = new ArrayList<>(hashRangeCount);
    // for each hash range index, we aggregate the metric data.
    IntStream.range(0, hashRangeCount).forEach(i ->
        aggregatedMetricData.add(i, metricData.values().stream().mapToLong(lst -> lst.get(i)).sum()));

    // Do the optimization using the information derived above.
    final Long totalSize = aggregatedMetricData.stream().mapToLong(n -> n).sum(); // get total size
    final Long idealSizePerTaskGroup = totalSize / taskGroupListSize; // and derive the ideal size per task group
    LOG.info("Skew: idealSizePerTaskgroup {} = {}(totalSize) / {}(taskGroupListSize)",
        idealSizePerTaskGroup, totalSize, taskGroupListSize);

    final double sizeBufferFactor = 0.1;
    final double sizeBuffer = idealSizePerTaskGroup * sizeBufferFactor;
    final long upperBoundSize = idealSizePerTaskGroup + (long) sizeBuffer;
    final long lowerBoundSize = idealSizePerTaskGroup - (long) sizeBuffer;
    LOG.info("upper {} lower {}", upperBoundSize, lowerBoundSize);

    // find HashRanges to apply (for each blocks of each block).
    final List<KeyRange> keyRanges = new ArrayList<>(taskGroupListSize);
    List<Long> sizePerTaskGroup = new ArrayList();
    int startingHashValue = 0;
    int finishingHashValue = 1; // initial values
    Long currentAccumulatedSize = 0L;
    for (int i = 1; i <= taskGroupListSize; i++) {
      if (i != taskGroupListSize) {
        final Long idealAccumulatedSize = idealSizePerTaskGroup * i; // where we should end
        // find the point while adding up one by one.
        while (currentAccumulatedSize < idealAccumulatedSize) {
          currentAccumulatedSize += aggregatedMetricData.get(finishingHashValue);
          LOG.info("Adding {}", aggregatedMetricData.get(finishingHashValue));
          finishingHashValue++;
        }

        long finalSize;
        if (i > 1) {
          long currentSize = currentAccumulatedSize - sizePerTaskGroup.stream().mapToLong(l -> l).sum();
          long oneStepBack = currentSize - aggregatedMetricData.get(finishingHashValue - 1);
          LOG.info("oneStepBack {}", oneStepBack);
          if (!(currentSize >= lowerBoundSize && currentSize <= upperBoundSize)) {
            if (oneStepBack >= lowerBoundSize) {
              finishingHashValue--;
              currentAccumulatedSize -= aggregatedMetricData.get(finishingHashValue);
            }
          }
        }

        if (i == 1) {
          finalSize = currentAccumulatedSize;
        } else {
          finalSize = currentAccumulatedSize - sizePerTaskGroup.stream().mapToLong(l -> l).sum();
        }

        sizePerTaskGroup.add(finalSize);
        LOG.info("Skew: resulting size {}", finalSize);

        // assign appropriately
        keyRanges.add(i - 1, HashRange.of(startingHashValue, finishingHashValue));
        LOG.info("Skew: resulting hashrange {} ~ {}, size {}",
            startingHashValue, finishingHashValue, currentAccumulatedSize);
        startingHashValue = finishingHashValue;
      } else { // last one: we put the range of the rest.
        keyRanges.add(i - 1, HashRange.of(startingHashValue, hashRangeCount));
        LOG.info("Skew: resulting hashrange {} ~ {}, size {}", startingHashValue, hashRangeCount,
            currentAccumulatedSize);
      }
    }

    return keyRanges;
  }
}
