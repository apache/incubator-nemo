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

import org.apache.nemo.common.KeyRange;
import org.apache.nemo.common.KeyExtractor;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.edge.executionproperty.PartitionSetProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceAntiAffinityProperty;
import org.apache.nemo.common.partitioner.HashPartitioner;
import org.apache.nemo.common.partitioner.Partitioner;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link SkewRunTimePass}.
 */
public class SkewRuntimePassTest {
  private final Map<Object, Long> testMetricData = new HashMap<>();
  private final static int DST_PARALLELISM = 2;
  private final static int NUM_PARTITIONS = 10;

  @Before
  public void setUp() {
    // Skewed partition size lists
    buildPartitionSizeList(Arrays.asList(5L, 5L, 10L, 50L, 110L, 5L, 5L, 10L, 50L, 100L));
    buildPartitionSizeList(Arrays.asList(5L, 10L, 5L, 0L, 0L, 5L, 10L, 5L, 0L, 0L));
    buildPartitionSizeList(Arrays.asList(10L, 5L, 5L, 0L, 0L, 10L, 5L, 5L, 0L, 0L));
    // Ideal distribution = Dst1-Total220[20, 20, 20, 50, 110], Dst2-Total210[20, 20, 20, 50, 100]
  }

  /**
   * Test DataSkewRuntimePass whether it redistributes skewed partitions
   * to partitions with approximate size of (total size / the number of tasks).
   */
  @Test
  public void testDataSkewDynamicOptimizationPass() {
    final KeyExtractor asIsExtractor = new AsIsKeyExtractor();
    final HashPartitioner partitioner = new HashPartitioner(NUM_PARTITIONS, asIsExtractor);

    final Pair<PartitionSetProperty, ResourceAntiAffinityProperty> resultPair = new SkewRunTimePass(1)
      .analyzeMessage(testMetricData, partitioner, NUM_PARTITIONS, DST_PARALLELISM);
    final List<KeyRange> keyRanges = resultPair.left().getValue();
    final HashSet<Integer> antiAfiinityGroup = resultPair.right().getValue();

    // Test whether it correctly redistributed hash ranges.
    assertEquals(0, keyRanges.get(0).rangeBeginInclusive());
    assertEquals(5, keyRanges.get(0).rangeEndExclusive());
    assertEquals(5, keyRanges.get(1).rangeBeginInclusive());
    assertEquals(10, keyRanges.get(1).rangeEndExclusive());

    // Test whether it caught the provided skewness.
    assertEquals(true, antiAfiinityGroup.contains(0));
    assertEquals(false, antiAfiinityGroup.contains(1));
  }

  /**
   * Builds a partition size metrics with given partition sizes for test.
   *
   * @param partitionSizes the size of partitions.
   * @return the partition size metrics.
   */
  private void buildPartitionSizeList(final List<Long> partitionSizes) {
    int key = 0;
    for (final long partitionSize : partitionSizes) {
      if (testMetricData.containsKey(key)) {
        testMetricData.compute(key, (existingKey, existingValue) -> existingValue + partitionSize);
      } else {
        testMetricData.put(key, partitionSize);
      }
      key++;
    }
  }

  /**
   * Custom {@link KeyExtractor} which returns the element as is.
   */
  private final class AsIsKeyExtractor implements KeyExtractor {

    /**
     * @see KeyExtractor#extractKey(Object).
     */
    @Override
    public Object extractKey(final Object element) {
      return element;
    }
  }
}
