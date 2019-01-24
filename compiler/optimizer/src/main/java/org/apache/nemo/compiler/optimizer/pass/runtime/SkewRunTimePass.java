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
package org.apache.nemo.compiler.optimizer.pass.runtime;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.KeyRange;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.NumOfPartitionProperty;
import org.apache.nemo.common.ir.edge.executionproperty.PartitionSetProperty;
import org.apache.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Dynamic optimization pass for handling data skew.
 * Using a map of key to partition size as a metric used for dynamic optimization,
 * this RunTimePass identifies a number of keys with big partition sizes(skewed key)
 * and evenly redistributes data via overwriting incoming edges of destination tasks.
 */
public final class SkewRunTimePass extends RunTimePass<Map<Object, Long>> {
  private static final Logger LOG = LoggerFactory.getLogger(SkewRunTimePass.class.getName());

  @Override
  public IRDAG optimize(final IRDAG irdag,
                        final Message<Map<Object, Long>> message) {
    // Validates target edges...
    final IREdge targetEdge = message.getExaminedEdge();
    final int numOfPartitions = targetEdge.getPropertyValue(NumOfPartitionProperty.class).get();
    final PartitionerProperty partitioner = targetEdge.getPropertyValue(PartitionerProperty.class).get();

    // Calculate keyRanges.
    final Map<Object, Long> messageValue = message.getMessageValue();
    final PartitionSetProperty evenPartitionSet = calculateKeyRanges(messageValue, numOfPartitions, partitioner);

    // Set the partitionSet property and return IRDAG.
    targetEdge.setPropertyPermanently(evenPartitionSet);
    return irdag;
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
  private PartitionSetProperty calculateKeyRanges(final Map<Object, Long> keyToCountMap,
                                                  final Integer numOfPartitions,
                                                  final Integer dstParallelism,
                                                  final Partitioner<Integer> partitioner) {
    final Map<Integer, Long> partitionKeyToPartitionCount = new HashMap<>();
    int lastKey = numOfPartitions - 1;
    // Aggregate the counts per each "partition key" assigned by Partitioner.

    for (final Map.Entry<Object, Long> entry : keyToCountMap.entrySet()) {
      final int partitionKey = partitioner.partition(entry.getKey());
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
        keyRanges.add(i - 1, KeyRange.of(startingKey, finishingKey));
        LOG.debug("KeyRange {}~{}, Size {}", startingKey, finishingKey - 1,
          currentAccumulatedSize - prevAccumulatedSize);

        prevAccumulatedSize = currentAccumulatedSize;
        startingKey = finishingKey;
      } else { // last one: we put the range of the rest.
        boolean isSkewedKey = containsSkewedSize(partitionSizeList, skewedSizes, startingKey, lastKey + 1);
        keyRanges.add(i - 1, KeyRange.of(startingKey, lastKey + 1));

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

  private List<Long> identifySkewedKeys(final List<Long> partitionSizeList) {
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
}
