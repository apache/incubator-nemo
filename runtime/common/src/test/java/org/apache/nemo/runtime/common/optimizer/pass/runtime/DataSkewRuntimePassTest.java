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

import org.apache.nemo.common.HashRange;
import org.apache.nemo.common.KeyExtractor;
import org.apache.nemo.common.KeyRange;
import org.apache.nemo.runtime.common.partitioner.HashPartitioner;
import org.apache.nemo.runtime.common.partitioner.Partitioner;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link DataSkewRuntimePass}.
 */
public class DataSkewRuntimePassTest {
  private final Map<Object, Long> testMetricData = new HashMap<>();

  @Before
  public void setUp() {
    // Skewed partition size lists
    buildPartitionSizeList(Arrays.asList(5L, 5L, 10L, 50L, 100L));
    buildPartitionSizeList(Arrays.asList(5L, 10L, 5L, 0L, 0L));
    buildPartitionSizeList(Arrays.asList(10L, 5L, 5L, 0L, 0L));
  }

  /**
   * Test DataSkewRuntimePass whether it redistributes skewed partitions
   * to partitions with approximate size of (total size / the number of tasks).
   */
  @Test
  public void testDataSkewDynamicOptimizationPass() {
    final Integer taskNum = 5;
    final KeyExtractor asIsExtractor = new AsIsKeyExtractor();
    final Partitioner partitioner = new HashPartitioner(taskNum, asIsExtractor);

    final List<KeyRange> keyRanges =
        new DataSkewRuntimePass(2).calculateKeyRanges(testMetricData, taskNum, partitioner);

    // Test whether it correctly redistributed hash ranges.
    assertEquals(0, keyRanges.get(0).rangeBeginInclusive());
    assertEquals(2, keyRanges.get(0).rangeEndExclusive());
    assertEquals(2, keyRanges.get(1).rangeBeginInclusive());
    assertEquals(3, keyRanges.get(1).rangeEndExclusive());
    assertEquals(3, keyRanges.get(2).rangeBeginInclusive());
    assertEquals(4, keyRanges.get(2).rangeEndExclusive());
    assertEquals(4, keyRanges.get(3).rangeBeginInclusive());
    assertEquals(5, keyRanges.get(3).rangeEndExclusive());
    assertEquals(5, keyRanges.get(4).rangeBeginInclusive());
    assertEquals(5, keyRanges.get(4).rangeEndExclusive());

    // Test whether it caught the provided skewness.
    assertEquals(false, ((HashRange)keyRanges.get(0)).isSkewed());
    assertEquals(false, ((HashRange)keyRanges.get(1)).isSkewed());
    assertEquals(true, ((HashRange)keyRanges.get(2)).isSkewed());
    assertEquals(true, ((HashRange)keyRanges.get(3)).isSkewed());
    assertEquals(false, ((HashRange)keyRanges.get(4)).isSkewed());
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
