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
package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * This tracks the minimum input watermark among multiple input streams.
 */
public final class MultiInputWatermarkManager implements InputWatermarkManager {
  private static final Logger LOG = LoggerFactory.getLogger(MultiInputWatermarkManager.class.getName());

  private final List<Watermark> watermarks;
  private final OutputCollector<?> watermarkCollector;
  private int minWatermarkIndex;
  public MultiInputWatermarkManager(final int numEdges,
                                    final OutputCollector<?> watermarkCollector) {
    super();
    this.watermarks = new ArrayList<>(numEdges);
    this.watermarkCollector = watermarkCollector;
    this.minWatermarkIndex = 0;
    // We initialize watermarks as min value because
    // we should not emit watermark until all edges emit watermarks.
    for (int i = 0; i < numEdges; i++) {
      watermarks.add(new Watermark(Long.MIN_VALUE));
    }
  }

  private int findNextMinWatermarkIndex() {
    int index = -1;
    long timestamp = Long.MAX_VALUE;
    for (int i = 0; i < watermarks.size(); i++) {
      if (watermarks.get(i).getTimestamp() < timestamp) {
        index = i;
        timestamp = watermarks.get(i).getTimestamp();
      }
    }
    return index;
  }

  @Override
  public void trackAndEmitWatermarks(final int edgeIndex, final Watermark watermark) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Track watermark {} emitted from edge {}:, {}", watermark.getTimestamp(), edgeIndex,
        watermarks.toString());
    }

    if (edgeIndex == minWatermarkIndex) {
      // update min watermark
      final Watermark prevMinWatermark = watermarks.get(minWatermarkIndex);
      watermarks.set(minWatermarkIndex, watermark);
       // find min watermark
      minWatermarkIndex = findNextMinWatermarkIndex();
      final Watermark minWatermark = watermarks.get(minWatermarkIndex);

      if (minWatermark.getTimestamp() < prevMinWatermark.getTimestamp()) {
        throw new IllegalStateException(
          "The current min watermark is ahead of prev min: " + minWatermark + ", " + prevMinWatermark);
      }

      if (minWatermark.getTimestamp() > prevMinWatermark.getTimestamp()) {
        // Watermark timestamp progress!
        // Emit the min watermark
        if (LOG.isDebugEnabled()) {
          LOG.debug("Emit watermark {}, {}", minWatermark, watermarks);
        }
        watermarkCollector.emitWatermark(minWatermark);
      }
    } else {
      // The recent watermark timestamp cannot be less than the previous one
      // because watermark is monotonically increasing.
      if (watermarks.get(edgeIndex).getTimestamp() > watermark.getTimestamp()) {
        throw new IllegalStateException(
          "The recent watermark timestamp cannot be less than the previous one "
            + "because watermark is monotonically increasing.");
      }
      watermarks.set(edgeIndex, watermark);
    }
  }
}
