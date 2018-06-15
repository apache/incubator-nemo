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
package edu.snu.nemo.runtime.common.optimizer.pass.runtime;

import com.google.common.annotations.VisibleForTesting;
import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.eventhandler.RuntimeEventHandler;
import edu.snu.nemo.common.exception.DynamicOptimizationException;

import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.data.KeyRange;
import edu.snu.nemo.runtime.common.plan.PhysicalPlan;
import edu.snu.nemo.runtime.common.plan.Stage;
import edu.snu.nemo.runtime.common.plan.StageEdge;
import edu.snu.nemo.runtime.common.data.HashRange;
import edu.snu.nemo.runtime.common.eventhandler.DynamicOptimizationEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Dynamic optimization pass for handling data skew.
 * It receives pairs of the key index and the size of a partition for each output block.
 */
public final class DataSkewRuntimePass implements RuntimePass<Pair<List<String>, Map<Integer, Long>>> {
  private static final Logger LOG = LoggerFactory.getLogger(DataSkewRuntimePass.class.getName());
  private final Set<Class<? extends RuntimeEventHandler>> eventHandlers;

  /**
   * Constructor.
   */
  public DataSkewRuntimePass() {
    this.eventHandlers = Collections.singleton(
        DynamicOptimizationEventHandler.class);
  }

  @Override
  public Set<Class<? extends RuntimeEventHandler>> getEventHandlerClasses() {
    return this.eventHandlers;
  }

  @Override
  public PhysicalPlan apply(final PhysicalPlan originalPlan,
                            final Pair<List<String>, Map<Integer, Long>> metricData) {
    // Builder to create new stages.
    final DAGBuilder<Stage, StageEdge> physicalDAGBuilder =
        new DAGBuilder<>(originalPlan.getStageDAG());
    final List<String> blockIds = metricData.left();

    // get edges to optimize
    final List<String> optimizationEdgeIds = blockIds.stream().map(blockId ->
        RuntimeIdGenerator.getRuntimeEdgeIdFromBlockId(blockId)).collect(Collectors.toList());
    final DAG<Stage, StageEdge> stageDAG = originalPlan.getStageDAG();
    final List<StageEdge> optimizationEdges = stageDAG.getVertices().stream()
        .flatMap(stage -> stageDAG.getIncomingEdgesOf(stage).stream())
        .filter(stageEdge -> optimizationEdgeIds.contains(stageEdge.getId()))
        .collect(Collectors.toList());

    // Get number of evaluators of the next stage (number of blocks).
    final Integer taskListSize = optimizationEdges.stream().findFirst().orElseThrow(() ->
        new RuntimeException("optimization edges are empty")).getDst().getTaskIds().size();

    // Calculate keyRanges.
    final List<KeyRange> keyRanges = calculateHashRanges(metricData.right(), taskListSize);

    // Overwrite the previously assigned hash value range in the physical DAG with the new range.
    optimizationEdges.forEach(optimizationEdge -> {
      // Update the information.
      final List<KeyRange> taskIdxToHashRange = new ArrayList<>();
      IntStream.range(0, taskListSize).forEach(i -> taskIdxToHashRange.add(keyRanges.get(i)));
      optimizationEdge.setTaskIdxToKeyRange(taskIdxToHashRange);
    });

    return new PhysicalPlan(originalPlan.getId(), physicalDAGBuilder.build(), originalPlan.getIdToIRVertex());
  }

  /**
   * Method for calculating key ranges to evenly distribute the skewed metric data.
   *
   * @param aggregatedMetricData the metric data.
   * @param taskListSize the size of the task list.
   * @return the list of key ranges calculated.
   */
  @VisibleForTesting
  public List<KeyRange> calculateHashRanges(final Map<Integer, Long> aggregatedMetricData,
                                            final Integer taskListSize) {
    // NOTE: aggregatedMetricDataMap is made up of a map of (hash value, blockSize).
    // Get the max hash value.
    final int maxHashValue = aggregatedMetricData.keySet().stream()
        .max(Integer::compareTo)
        .orElseThrow(() -> new DynamicOptimizationException("Cannot find max hash value among blocks."));

    // Do the optimization using the information derived above.
    final Long totalSize = aggregatedMetricData.values().stream().mapToLong(n -> n).sum(); // get total size
    final Long idealSizePerTask = totalSize / taskListSize; // and derive the ideal size per task
    LOG.info("idealSizePerTask {} = {}(totalSize) / {}(taskListSize)",
        idealSizePerTask, totalSize, taskListSize);

    // find HashRanges to apply (for each blocks of each block).
    final List<KeyRange> keyRanges = new ArrayList<>(taskListSize);
    int startingHashValue = 0;
    int finishingHashValue = 1; // initial values
    Long currentAccumulatedSize = aggregatedMetricData.getOrDefault(startingHashValue, 0L);
    for (int i = 1; i <= taskListSize; i++) {
      if (i != taskListSize) {
        final Long idealAccumulatedSize = idealSizePerTask * i; // where we should end
        // find the point while adding up one by one.
        while (currentAccumulatedSize < idealAccumulatedSize) {
          currentAccumulatedSize += aggregatedMetricData.getOrDefault(finishingHashValue, 0L);
          finishingHashValue++;
        }

        final Long oneStepBack =
            currentAccumulatedSize - aggregatedMetricData.getOrDefault(finishingHashValue - 1, 0L);
        final Long diffFromIdeal = currentAccumulatedSize - idealAccumulatedSize;
        final Long diffFromIdealOneStepBack = idealAccumulatedSize - oneStepBack;
        // Go one step back if we came too far.
        if (diffFromIdeal > diffFromIdealOneStepBack) {
          finishingHashValue--;
          currentAccumulatedSize -= aggregatedMetricData.getOrDefault(finishingHashValue, 0L);
        }

        // assign appropriately
        keyRanges.add(i - 1, HashRange.of(startingHashValue, finishingHashValue));
        startingHashValue = finishingHashValue;
      } else { // last one: we put the range of the rest.
        keyRanges.add(i - 1, HashRange.of(startingHashValue, maxHashValue + 1));
      }
    }
    return keyRanges;
  }
}
