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

import org.apache.nemo.common.HashRange;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.KeyRange;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.PartitionSetProperty;
import org.apache.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.MinParallelismProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceAntiAffinityProperty;
import org.apache.nemo.common.partitioner.HashPartitioner;
import org.apache.nemo.common.partitioner.Partitioner;
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
  private static final int DEFAULT_NUM_SKEWED_TASKS = 1;

  private final int numSkewedTasks;

  public SkewRunTimePass() {
    this(DEFAULT_NUM_SKEWED_TASKS);
  }

  public SkewRunTimePass(final int numOfSkewedKeys) {
    this.numSkewedTasks = numOfSkewedKeys;
  }



  @Override
  public IRDAG apply(final IRDAG irdag, final Message<Map<Object, Long>> message) {
    // The message was produced to examine this edge.
    final Set<IREdge> edges = message.getExaminedEdges();

    final IREdge representativeEdge = edges.iterator().next();

    // Use the following execution properties.
    final Pair<PartitionerProperty.Type, Integer> partitionerProperty =
      representativeEdge.getPropertyValue(PartitionerProperty.class).get();
    final int dstParallelism = representativeEdge.getDst().getPropertyValue(MinParallelismProperty.class).get();

    // Compute the optimal partition distribution, using the message value.
    final Map<Object, Long> messageValue = message.getMessageValue();
    final Pair<PartitionSetProperty, ResourceAntiAffinityProperty> pair = analyzeMessage(
      messageValue,
      (HashPartitioner) Partitioner.getPartitioner(
        representativeEdge.getExecutionProperties(), representativeEdge.getDst().getExecutionProperties()),
      partitionerProperty.right(),
      dstParallelism);

    // Set the partitionSet property
    edges.forEach(edge -> edge.setPropertyPermanently(pair.left()));
    representativeEdge.getDst().setPropertyPermanently(pair.right());

    // Return the IRDAG.
    return irdag;
  }

  /**
   * Evenly distribute the skewed data to the destination tasks.
   * Partition denotes for a keyed portion of a Task output.
   * Using a map of actual data key to count, this method gets the size of each the given partitions and
   * redistribute the key range of partitions with approximate size of (total size of partitions / the number of tasks).
   * Assumption: the returned key of the partitioner is always 0 or positive integer.
   *
   * @param keyToCountMap statistics.
   * @param partitioner used.
   * @param numOfPartitions created.
   * @param dstParallelism of the destination vertex.
   * @return an optimal PartitionSetProperty and a ResourceAntiAffinityProperty.
   */
  private Pair<PartitionSetProperty, ResourceAntiAffinityProperty> analyzeMessage(final Map<Object, Long> keyToCountMap,
                                                                                  final HashPartitioner partitioner,
                                                                                  final int numOfPartitions,
                                                                                  final int dstParallelism) {

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
    final ArrayList<KeyRange> keyRanges = new ArrayList<>();

    final HashSet<Integer> skewedTaskIndices = new HashSet<>();
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

        keyRanges.add(i - 1, HashRange.of(startingKey, finishingKey));
        LOG.debug("KeyRange {}~{}, Size {}", startingKey, finishingKey - 1,
          currentAccumulatedSize - prevAccumulatedSize);

        prevAccumulatedSize = currentAccumulatedSize;
        startingKey = finishingKey;
      } else { // last one: we put the range of the rest.
        keyRanges.add(i - 1, HashRange.of(startingKey, lastKey + 1));

        while (finishingKey <= lastKey) {
          currentAccumulatedSize += partitionSizeList.get(finishingKey);
          finishingKey++;
        }
        LOG.debug("KeyRange {}~{}, Size {}", startingKey, lastKey + 1,
          currentAccumulatedSize - prevAccumulatedSize);
      }

      boolean isSkewedKey = containsSkewedSize(partitionSizeList, skewedSizes, startingKey, finishingKey);
      if (isSkewedKey) {
        skewedTaskIndices.add(i - 1);
      }
    }

    return Pair.of(PartitionSetProperty.of(keyRanges), ResourceAntiAffinityProperty.of(skewedTaskIndices));
  }

  public List<Long> identifySkewedKeys(final List<Long> partitionSizeList) {
    // Identify skewed keys.
    List<Long> sortedMetricData = partitionSizeList.stream()
      .sorted(Comparator.reverseOrder())
      .collect(Collectors.toList());
    List<Long> skewedSizes = new ArrayList<>();
    for (int i = 0; i < numSkewedTasks; i++) {
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
