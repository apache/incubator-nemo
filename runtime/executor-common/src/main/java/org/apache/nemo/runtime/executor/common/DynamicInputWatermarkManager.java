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
package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.punctuation.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This tracks the minimum input watermark among multiple input streams.
 */
public final class DynamicInputWatermarkManager implements InputWatermarkManager {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicInputWatermarkManager.class.getName());

  private final ConcurrentMap<Integer, Watermark> taskWatermarkMap;
  private final OutputCollector<?> watermarkCollector;
  private int minWatermarkIndex;
  private Watermark currMinWatermark = new Watermark(Long.MIN_VALUE);
  private String sourceId;
  private final IRVertex vertex;

  public DynamicInputWatermarkManager(final IRVertex vertex,
                                      final OutputCollector<?> watermarkCollector) {
    super();
    this.vertex = vertex;
    this.taskWatermarkMap = new ConcurrentHashMap<>();
    this.watermarkCollector = watermarkCollector;
    this.minWatermarkIndex = 0;
  }

  private int findNextMinWatermarkIndex() {
    int index = -1;
    long timestamp = Long.MAX_VALUE;
    for (final Map.Entry<Integer, Watermark> entry : taskWatermarkMap.entrySet()) {
      if (entry.getValue().getTimestamp() < timestamp) {
        index = entry.getKey();
        timestamp = entry.getValue().getTimestamp();
      }
    }
    return index;
  }

  public synchronized void addEdge(final int index) {
    LOG.info("{} edge index added {} at {}", vertex.getId(), index);
    minWatermarkIndex = index;
  }

  public synchronized void removeEdge(final int index) {
    LOG.info("{} edge index removed {}", vertex.getId(), index);
    taskWatermarkMap.remove(index);
    if (minWatermarkIndex == index) {
      minWatermarkIndex = findNextMinWatermarkIndex();
      LOG.info("{} min index changed from {} to {}, watermark {}", vertex.getId(), index, minWatermarkIndex, currMinWatermark);
    }
    // do not change min watermark!
  }

  @Override
  public synchronized void trackAndEmitWatermarks(final int edgeIndex, final Watermark watermark) {
    //LOG.info("Watermark from {}: {} at {}, min: {}", edgeIndex, watermark, vertex.getId(), currMinWatermark);

    if (edgeIndex == minWatermarkIndex) { // update min watermark
      taskWatermarkMap.put(minWatermarkIndex, watermark);
       // find min watermark
      final int nextMinWatermarkIndex = findNextMinWatermarkIndex();
      final Watermark nextMinWatermark = taskWatermarkMap.get(nextMinWatermarkIndex);

      if (nextMinWatermark.getTimestamp() < currMinWatermark.getTimestamp()) {
        // it is possible
        minWatermarkIndex = nextMinWatermarkIndex;
        LOG.warn("{} watermark less than prev: {}, {} maybe due to the new edge index",
          vertex .getId(), currMinWatermark, nextMinWatermark);
      } else if (nextMinWatermark.getTimestamp() > currMinWatermark.getTimestamp()) {
        // Watermark timestamp progress!
        // Emit the min watermark
        minWatermarkIndex = nextMinWatermarkIndex;
        currMinWatermark = nextMinWatermark;

        if (LOG.isDebugEnabled()) {
          LOG.debug("Emit watermark {}", currMinWatermark);
        }
        watermarkCollector.emitWatermark(currMinWatermark);
      }
    } else {
      // The recent watermark timestamp cannot be less than the previous one
      // because watermark is monotonically increasing.
      if (taskWatermarkMap.getOrDefault(edgeIndex, new Watermark(-1))
        .getTimestamp() > watermark.getTimestamp()) {
        throw new IllegalStateException(
          "The recent watermark timestamp cannot be less than the previous one "
            + "because watermark is monotonically increasing.");
      }
      taskWatermarkMap.put(edgeIndex, watermark);
    }
  }

  @Override
  public void setWatermarkSourceId(String sid) {
    sourceId = sid;
  }

  @Override
  public String getWatermarkSourceId() {
    return sourceId;
  }
}
