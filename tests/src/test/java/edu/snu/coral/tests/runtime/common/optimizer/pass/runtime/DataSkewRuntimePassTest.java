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
package edu.snu.coral.tests.runtime.common.optimizer.pass.runtime;

import edu.snu.coral.runtime.common.data.KeyRange;
import edu.snu.coral.runtime.common.optimizer.pass.runtime.DataSkewRuntimePass;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link DataSkewRuntimePass}.
 */
public class DataSkewRuntimePassTest {
  private final Map<String, List<Long>> testMetricData = new HashMap<>();

  @Before
  public void setUp() {
    // Sum is 30 for each hashRanges: 0-3, 3-5, 5-7, 7-9, 9-10.
    testMetricData.put("1", Arrays.asList(1L, 2L, 4L, 2L, 1L, 8L, 2L, 4L, 2L, 10L));
    testMetricData.put("2", Arrays.asList(3L, 5L, 5L, 7L, 10L, 3L, 5L, 4L, 8L, 5L));
    testMetricData.put("3", Arrays.asList(2L, 3L, 5L, 5L, 5L, 6L, 6L, 8L, 4L, 15L));
  }

  /**
   * Test if the test case suggested above works correctly.
   */
  @Test
  public void testDataSkewDynamicOptimizationPass() {
    final Integer taskGroupListSize = 5;

    final List<KeyRange> keyRanges =
        new DataSkewRuntimePass().calculateHashRanges(testMetricData, taskGroupListSize);

    assertEquals(0, keyRanges.get(0).rangeBeginInclusive());
    assertEquals(3, keyRanges.get(0).rangeEndExclusive());
    assertEquals(3, keyRanges.get(1).rangeBeginInclusive());
    assertEquals(5, keyRanges.get(1).rangeEndExclusive());
    assertEquals(5, keyRanges.get(2).rangeBeginInclusive());
    assertEquals(7, keyRanges.get(2).rangeEndExclusive());
    assertEquals(7, keyRanges.get(3).rangeBeginInclusive());
    assertEquals(9, keyRanges.get(3).rangeEndExclusive());
    assertEquals(9, keyRanges.get(4).rangeBeginInclusive());
    assertEquals(10, keyRanges.get(4).rangeEndExclusive());
  }
}
