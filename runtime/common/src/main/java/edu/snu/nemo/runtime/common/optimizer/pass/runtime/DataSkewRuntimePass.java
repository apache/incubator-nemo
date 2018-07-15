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
import edu.snu.nemo.common.DataSkewMetricFactory;
import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.eventhandler.RuntimeEventHandler;
import edu.snu.nemo.common.exception.DynamicOptimizationException;

import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataSkewMetricProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.common.KeyRange;
import edu.snu.nemo.common.HashRange;
import edu.snu.nemo.runtime.common.eventhandler.DynamicOptimizationEventHandler;
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
public final class DataSkewRuntimePass implements RuntimePass<Pair<List<String>, Map<Integer, Long>>> {
  private static final Logger LOG = LoggerFactory.getLogger(DataSkewRuntimePass.class.getName());
  private final Set<Class<? extends RuntimeEventHandler>> eventHandlers;
  // Skewed keys denote for top n keys in terms of partition size.
  private static final int DEFAULT_NUM_SKEWED_KEYS = 3;
  private int numSkewedKeys = DEFAULT_NUM_SKEWED_KEYS;

  /**
   * Constructor.
   */
  public DataSkewRuntimePass() {
    this.eventHandlers = Collections.singleton(
        DynamicOptimizationEventHandler.class);
  }

  public DataSkewRuntimePass setNumSkewedKeys(final int numOfSkewedKeys) {
    numSkewedKeys = numOfSkewedKeys;
    return this;
  }

  @Override
  public Set<Class<? extends RuntimeEventHandler>> getEventHandlerClasses() {
    return this.eventHandlers;
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> irDAG,
                            final Pair<List<String>, Map<Integer, Long>> metricData) {
    final List<String> blockIds = metricData.left();

    // get edges to optimize
    final List<String> optimizationEdgeIds = blockIds.stream().map(blockId ->
        RuntimeIdGenerator.getRuntimeEdgeIdFromBlockId(blockId)).collect(Collectors.toList());
    final List<IREdge> optimizationEdges = irDAG.getVertices().stream()
        .flatMap(v -> irDAG.getIncomingEdgesOf(v).stream())
        .filter(e -> optimizationEdgeIds.contains(e.getId()))
        .collect(Collectors.toList());

    // Get number of evaluators of the next stage (number of blocks).
    final IREdge targetEdge = optimizationEdges.stream().findFirst()
        .orElseThrow(() -> new RuntimeException("optimization edges are empty"));
    final Integer dstParallelism = targetEdge.getDst().getPropertyValue(ParallelismProperty.class).get();
    // Calculate keyRanges.
    final List<KeyRange> keyRanges = calculateKeyRanges(metricData.right(), dstParallelism);
    final Map<Integer, KeyRange> taskIdxToKeyRange = new HashMap<>();
    for (int i = 0; i < dstParallelism; i++) {
      taskIdxToKeyRange.put(i, keyRanges.get(i));
    }
    // Overwrite the previously assigned key range in the physical DAG with the new range.
    targetEdge.setProperty(DataSkewMetricProperty.of(new DataSkewMetricFactory(taskIdxToKeyRange)));
    return irDAG;
  }

  public List<Integer> identifySkewedKeys(final Map<Integer, Long> keyValToPartitionSizeMap) {
    // Identify skewed keyes.
    List<Map.Entry<Integer, Long>> sortedMetricData = keyValToPartitionSizeMap.entrySet().stream()
        .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
        .collect(Collectors.toList());
    List<Integer> skewedKeys = new ArrayList<>();
    for (int i = 0; i < numSkewedKeys; i++) {
      skewedKeys.add(sortedMetricData.get(i).getKey());
      LOG.info("Skewed key: Key {} Size {}", sortedMetricData.get(i).getKey(), sortedMetricData.get(i).getValue());
    }

    return skewedKeys;
  }

  private boolean containsSkewedKey(final List<Integer> skewedKeys,
                                    final int startingKey, final int finishingKey) {
    for (int k = startingKey; k < finishingKey; k++) {
      if (skewedKeys.contains(k)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Evenly distribute the skewed data to the destination tasks.
   * Partition denotes for a keyed portion of a Task output, whose key is a key.
   * Using a map of key to partition size, this method groups the given partitions
   * to a key range of partitions with approximate size of (total size of partitions / the number of tasks).
   *
   * @param keyToPartitionSizeMap a map of key to partition size.
   * @param dstParallelism the number of tasks that receives this data as input.
   * @return the list of key ranges calculated.
   */
  @VisibleForTesting
  public List<KeyRange> calculateKeyRanges(final Map<Integer, Long> keyToPartitionSizeMap,
                                           final Integer dstParallelism) {
    // Get the biggest key.
    final int maxKey = keyToPartitionSizeMap.keySet().stream()
        .max(Integer::compareTo)
        .orElseThrow(() -> new DynamicOptimizationException("Cannot find max key among blocks."));

    // Identify skewed keys, which is top numSkewedKeys number of keys.
    List<Integer> skewedKeys = identifySkewedKeys(keyToPartitionSizeMap);

    // Calculate the ideal size for each destination task.
    final Long totalSize = keyToPartitionSizeMap.values().stream().mapToLong(n -> n).sum(); // get total size
    final Long idealSizePerTask = totalSize / dstParallelism; // and derive the ideal size per task

    final List<KeyRange> keyRanges = new ArrayList<>(dstParallelism);
    int startingKey = 0;
    int finishingKey = 1;
    Long currentAccumulatedSize = keyToPartitionSizeMap.getOrDefault(startingKey, 0L);
    Long prevAccumulatedSize = 0L;
    for (int i = 1; i <= dstParallelism; i++) {
      if (i != dstParallelism) {
        // Ideal accumulated partition size for this task.
        final Long idealAccumulatedSize = idealSizePerTask * i;
        // By adding partition sizes, find the accumulated size nearest to the given ideal size.
        while (currentAccumulatedSize < idealAccumulatedSize) {
          currentAccumulatedSize += keyToPartitionSizeMap.getOrDefault(finishingKey, 0L);
          finishingKey++;
        }

        final Long oneStepBack =
            currentAccumulatedSize - keyToPartitionSizeMap.getOrDefault(finishingKey - 1, 0L);
        final Long diffFromIdeal = currentAccumulatedSize - idealAccumulatedSize;
        final Long diffFromIdealOneStepBack = idealAccumulatedSize - oneStepBack;
        // Go one step back if we came too far.
        if (diffFromIdeal > diffFromIdealOneStepBack) {
          finishingKey--;
          currentAccumulatedSize -= keyToPartitionSizeMap.getOrDefault(finishingKey, 0L);
        }

        boolean isSkewedKey = containsSkewedKey(skewedKeys, startingKey, finishingKey);
        keyRanges.add(i - 1, HashRange.of(startingKey, finishingKey, isSkewedKey));
        LOG.debug("KeyRange {}~{}, Size {}", startingKey, finishingKey - 1,
            currentAccumulatedSize - prevAccumulatedSize);

        prevAccumulatedSize = currentAccumulatedSize;
        startingKey = finishingKey;
      } else { // last one: we put the range of the rest.
        boolean isSkewedKey = containsSkewedKey(skewedKeys, startingKey, finishingKey);
        keyRanges.add(i - 1,
            HashRange.of(startingKey, maxKey + 1, isSkewedKey));

        while (finishingKey <= maxKey) {
          currentAccumulatedSize += keyToPartitionSizeMap.getOrDefault(finishingKey, 0L);
          finishingKey++;
        }
        LOG.debug("KeyRange {}~{}, Size {}", startingKey, maxKey + 1,
            currentAccumulatedSize - prevAccumulatedSize);
      }
    }
    return keyRanges;
  }
}
