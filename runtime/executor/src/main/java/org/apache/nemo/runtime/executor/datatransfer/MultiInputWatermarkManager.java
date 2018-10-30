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

import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.punctuation.Watermark;

/**
 * This tracks the minimum input watermark among multiple input streams.
 */
public final class MultiInputWatermarkManager implements InputWatermarkManager {
  private final Watermark[] watermarks;
  private final OperatorVertex nextOperator;
  private int minWatermarkIndex;
  public MultiInputWatermarkManager(final int numEdges,
                                    final OperatorVertex nextOperator) {
    super();
    this.watermarks = new Watermark[numEdges];
    this.nextOperator = nextOperator;
    this.minWatermarkIndex = 0;
     // We initialize watermarks as min value because
    // we should not emit watermark until all edges emit watermarks.
    for (int i = 0; i < numEdges; i++) {
      watermarks[i] = new Watermark(Long.MIN_VALUE);
    }
  }

  private int findNextMinWatermarkIndex() {
    int index = -1;
    long timestamp = Long.MAX_VALUE;
    for (int i = 0; i < watermarks.length; i++) {
      if (watermarks[i].getTimestamp() < timestamp) {
        index = i;
        timestamp = watermarks[i].getTimestamp();
      }
    }
    return index;
  }

  public void trackAndEmitWatermarks(final int edgeIndex, final Watermark watermark) {
    if (edgeIndex == minWatermarkIndex) {
      // update min watermark
      final Watermark prevMinWatermark = watermarks[minWatermarkIndex];
      watermarks[minWatermarkIndex] = watermark;
       // find min watermark
      minWatermarkIndex = findNextMinWatermarkIndex();
      final Watermark minWatermark = watermarks[minWatermarkIndex];
      assert minWatermark.getTimestamp() >= prevMinWatermark.getTimestamp();
       if (minWatermark.getTimestamp() > prevMinWatermark.getTimestamp()) {
        // Watermark timestamp progress!
        // Emit the min watermark
        nextOperator.getTransform().onWatermark(minWatermark);
      }
    } else {
      // The recent watermark timestamp cannot be less than the previous one
      // because watermark is monotonically increasing.
      assert watermarks[edgeIndex].getTimestamp() <= watermark.getTimestamp();
      watermarks[edgeIndex] = watermark;
    }
  }
}
