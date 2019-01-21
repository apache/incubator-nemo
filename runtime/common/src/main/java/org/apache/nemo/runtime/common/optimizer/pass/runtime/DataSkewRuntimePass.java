/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.common.optimizer.pass.runtime;

import com.google.common.annotations.VisibleForTesting;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.eventhandler.RuntimeEventHandler;

import org.apache.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.KeyRange;
import org.apache.nemo.common.HashRange;
import org.apache.nemo.runtime.common.eventhandler.DynamicOptimizationEventHandler;
import org.apache.nemo.runtime.common.partitioner.DataSkewHashPartitioner;
import org.apache.nemo.runtime.common.partitioner.Partitioner;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.Stage;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Dynamic optimization pass for handling data skew.
 * Using a map of key to partition size as a metric used for dynamic optimization,
 * this RuntimePass identifies a number of keys with big partition sizes(skewed key)
 * and evenly redistributes data via overwriting incoming edges of destination tasks.
 */
public final class DataSkewRuntimePass extends RuntimePass<Pair<Set<StageEdge>, Map<Object, Long>>> {
  private static final Logger LOG = LoggerFactory.getLogger(DataSkewRuntimePass.class.getName());
  private static final int DEFAULT_NUM_SKEWED_KEYS = 1;
  /*
   * Hash range multiplier.
   * If we need to split or recombine an output data from a task after it is stored,
   * we multiply the hash range with this factor in advance
   * to prevent the extra deserialize - rehash - serialize process.
   * In these cases, the hash range will be (hash range multiplier X destination task parallelism).
   * The reason why we do not divide the output into a fixed number is that the fixed number can be smaller than
   * the destination task parallelism.
   */
  public static final int HASH_RANGE_MULTIPLIER = 10;

  private final Set<Class<? extends RuntimeEventHandler>> eventHandlers;
  // Skewed keys denote for top n keys in terms of partition size.
  private final int numSkewedKeys;

  /**
   * Constructor without expected number of skewed keys.
   */
  public DataSkewRuntimePass() {
    this(DEFAULT_NUM_SKEWED_KEYS);
  }

  /**
   * Constructor with expected number of skewed keys.
   *
   * @param numOfSkewedKeys the expected number of skewed keys.
   */
  public DataSkewRuntimePass(final int numOfSkewedKeys) {
    this.eventHandlers = Collections.singleton(DynamicOptimizationEventHandler.class);
    this.numSkewedKeys = numOfSkewedKeys;
  }

  @Override
  public Set<Class<? extends RuntimeEventHandler>> getEventHandlerClasses() {
    return this.eventHandlers;
  }

  @Override
  public PhysicalPlan apply(final PhysicalPlan originalPlan,
                            final Pair<Set<StageEdge>, Map<Object, Long>> metricData) {
    final Set<StageEdge> targetEdges = metricData.left();
    // Get number of evaluators of the next stage (number of blocks).
    final Integer dstParallelism = targetEdge.getDst().getPropertyValue(ParallelismProperty.class).
        orElseThrow(() -> new RuntimeException("No parallelism on a vertex"));
    if (!PartitionerProperty.Value.DataSkewHashPartitioner
      .equals(targetEdge.getPropertyValue(PartitionerProperty.class)
        .orElseThrow(() -> new RuntimeException("No partitioner property!")))) {
      throw new RuntimeException("Invalid partitioner is assigned to the target edge!");
    }
    final DataSkewHashPartitioner partitioner = (DataSkewHashPartitioner) Partitioner.getPartitioner(targetEdge);

    // Calculate keyRanges.
    final List<KeyRange> keyRanges = calculateKeyRanges(metricData.right(), dstParallelism, partitioner);
    final Map<Integer, KeyRange> taskIdxToKeyRange = new HashMap<>();
    for (int i = 0; i < dstParallelism; i++) {
      taskIdxToKeyRange.put(i, keyRanges.get(i));
    }

    // Overwrite the previously assigned key range in the physical DAG with the new range.
    final DAG<Stage, StageEdge> stageDAG = originalPlan.getStageDAG();
    for (Stage stage : stageDAG.getVertices()) {
      List<StageEdge> stageEdges = stageDAG.getOutgoingEdgesOf(stage);
      for (StageEdge edge : stageEdges) {
        if (edge.equals(targetEdge)) {
          edge.setTaskIdxToKeyRange(taskIdxToKeyRange);
        }
      }
    }

    return new PhysicalPlan(originalPlan.getPlanId(), stageDAG);
  }

  public List<Long> identifySkewedKeys(final List<Long> partitionSizeList) {
    // Identify skewed keys.
    List<Long> sortedMetricData = partitionSizeList.stream()
        .sorted(Comparator.reverseOrder())
        .collect(Collectors.toList());
    List<Long> skewedSizes = new ArrayList<>();
    for (int i = 0; i < numSkewedKeys; i++) {
      skewedSizes.add(sortedMetricData.get(i));
      LOG.info("Skewed size: {}", sortedMetricData.get(i));
    }

    return skewedSizes;
  }

  private boolean containsSkewedSize(final List<Long> partitionSizeList,
                                     final List<Long> skewedKeys,
                                     final int startingKey, final int finishingKey) {
    for (int i = startingKey; i < finishingKey; i++) {
      if (skewedKeys.contains(partitionSizeList.get(i))) {
        return true;
      }
    }

    return false;
  }

  /**
   * Evenly distribute the skewed data to the destination tasks.
   * Partition denotes for a keyed portion of a Task output.
   * Using a map of actual data key to count, this method gets the size of each the given partitions and
   * redistribute the key range of partitions with approximate size of (total size of partitions / the number of tasks).
   * Assumption: the returned key of the partitioner is always 0 or positive integer.
   *
   * @param keyToCountMap  a map of actual key to count.
   * @param dstParallelism the number of tasks that receive this data as input.
   * @param partitioner    the partitioner.
   * @return the list of key ranges calculated.
   */
  @VisibleForTesting
  public List<KeyRange> calculateKeyRanges(final Map<Object, Long> keyToCountMap,
                                           final Integer dstParallelism,
                                           final Partitioner<Integer> partitioner) {
    final Map<Integer, Long> partitionKeyToPartitionCount = new HashMap<>();
    int lastKey = 0;
    // Aggregate the counts per each "partition key" assigned by Partitioner.

    for (final Map.Entry<Object, Long> entry : keyToCountMap.entrySet()) {
      final int partitionKey = partitioner.partition(entry.getKey());
      lastKey = Math.max(lastKey, partitionKey);
      partitionKeyToPartitionCount.compute(partitionKey,
        (existPartitionKey, prevCount) -> (prevCount == null) ? entry.getValue() : prevCount + entry.getValue());
    }

    final List<Long> partitionSizeList = new ArrayList<>(lastKey + 1);
    for (int i = 0; i <= lastKey; i++) {
      final long countsForKey = partitionKeyToPartitionCount.getOrDefault(i, 0L);
      partitionSizeList.add(countsForKey);
    }

    // Identify skewed sizes, which is top numSkewedKeys number of keys.
    final List<Long> skewedSizes = identifySkewedKeys(partitionSizeList);

    // Calculate the ideal size for each destination task.
    final Long totalSize = partitionSizeList.stream().mapToLong(n -> n).sum(); // get total size
    final Long idealSizePerTask = totalSize / dstParallelism; // and derive the ideal size per task

    final List<KeyRange> keyRanges = new ArrayList<>(dstParallelism);
    int startingKey = 0;
    int finishingKey = 1;
    Long currentAccumulatedSize = partitionSizeList.get(startingKey);
    Long prevAccumulatedSize = 0L;
    for (int i = 1; i <= dstParallelism; i++) {
      if (i != dstParallelism) {
        // Ideal accumulated partition size for this task.
        final Long idealAccumulatedSize = idealSizePerTask * i;
        // By adding partition sizes, find the accumulated size nearest to the given ideal size.
        while (currentAccumulatedSize < idealAccumulatedSize) {
          currentAccumulatedSize += partitionSizeList.get(finishingKey);
          finishingKey++;
        }

        final Long oneStepBack =
            currentAccumulatedSize - partitionSizeList.get(finishingKey - 1);
        final Long diffFromIdeal = currentAccumulatedSize - idealAccumulatedSize;
        final Long diffFromIdealOneStepBack = idealAccumulatedSize - oneStepBack;
        // Go one step back if we came too far.
        if (diffFromIdeal > diffFromIdealOneStepBack) {
          finishingKey--;
          currentAccumulatedSize -= partitionSizeList.get(finishingKey);
        }

        boolean isSkewedKey = containsSkewedSize(partitionSizeList, skewedSizes, startingKey, finishingKey);
        keyRanges.add(i - 1, HashRange.of(startingKey, finishingKey, isSkewedKey));
        LOG.debug("KeyRange {}~{}, Size {}", startingKey, finishingKey - 1,
            currentAccumulatedSize - prevAccumulatedSize);

        prevAccumulatedSize = currentAccumulatedSize;
        startingKey = finishingKey;
      } else { // last one: we put the range of the rest.
        boolean isSkewedKey = containsSkewedSize(partitionSizeList, skewedSizes, startingKey, lastKey + 1);
        keyRanges.add(i - 1,
            HashRange.of(startingKey, lastKey + 1, isSkewedKey));

        while (finishingKey <= lastKey) {
          currentAccumulatedSize += partitionSizeList.get(finishingKey);
          finishingKey++;
        }
        LOG.debug("KeyRange {}~{}, Size {}", startingKey, lastKey + 1,
            currentAccumulatedSize - prevAccumulatedSize);
      }
    }
    return keyRanges;
  }
}
