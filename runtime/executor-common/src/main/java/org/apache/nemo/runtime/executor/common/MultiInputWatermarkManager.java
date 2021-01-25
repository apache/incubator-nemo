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
import org.joda.time.Instant;
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
  private Watermark currMinWatermark = new Watermark(Long.MIN_VALUE);
  private String sourceId;
  private final IRVertex vertex;
  private final String taskId;

  public MultiInputWatermarkManager(final IRVertex vertex,
                                    final int numEdges,
                                    final OutputCollector<?> watermarkCollector,
                                    final String taskId) {
    super();
    this.vertex = vertex;
    this.watermarks = new ArrayList<>(numEdges);
    this.watermarkCollector = watermarkCollector;
    this.minWatermarkIndex = 0;
    this.taskId = taskId;
    LOG.info("Number of edges for multi input watermark: " + numEdges);
    // We initialize watermarks as min value because
    // we should not emit watermark until all edges emit watermarks.
    for (int i = 0; i < numEdges; i++) {
      watermarks.add(new Watermark(Long.MIN_VALUE));
    }

    LOG.info("MultiInputWatermarkManager start");
  }

  public MultiInputWatermarkManager(final IRVertex vertex,
                                    final int numEdges,
                                    final OutputCollector<?> watermarkCollector) {
    this(vertex, numEdges, watermarkCollector, "");
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

  private void printWatermarks() {
    for (int i = 0; i < watermarks.size(); i++) {
      LOG.info("[{}: {}]", i, new Instant(watermarks.get(i).getTimestamp()));
    }
  }

  @Override
  public void trackAndEmitWatermarks(final int edgeIndex, final Watermark watermark) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Track watermark {} emitted from edge {}:, {}", watermark.getTimestamp(), edgeIndex,
        watermarks.toString());
    }

    /*
    if (vertex != null) {
      LOG.info("Watermark from {}: {} at {}, min: {}, minIndex: {}, task {}", edgeIndex, new Instant(watermark.getTimestamp()), vertex.getId(),
        new Instant(currMinWatermark.getTimestamp()),
        minWatermarkIndex, taskId);
      //LOG.info("Print watermarks");
      //printWatermarks();
    }
    */

    /*
    if (vertex != null) {
      LOG.info("At {} Track watermark {} emitted from edge {}", vertex.getId(), new Instant(watermark.getTimestamp()), edgeIndex);
      LOG.info("Print watermarks: ");
      printWatermarks();
    }
    */

    if (edgeIndex == minWatermarkIndex) {
      // update min watermark
      watermarks.set(minWatermarkIndex, watermark);

       // find min watermark
      final int nextMinWatermarkIndex = findNextMinWatermarkIndex();
      final Watermark nextMinWatermark = watermarks.get(nextMinWatermarkIndex);

      if (nextMinWatermark.getTimestamp() <= currMinWatermark.getTimestamp()) {
        // it is possible in the first time
        minWatermarkIndex = nextMinWatermarkIndex;
        //LOG.warn("{} watermark less than prev: {}, {} maybe due to the new edge index",
        //  vertex.getId(), new Instant(currMinWatermark.getTimestamp()), new Instant(nextMinWatermark.getTimestamp()));
      } else if (nextMinWatermark.getTimestamp() > currMinWatermark.getTimestamp()) {
        // Watermark timestamp progress!
        // Emit the min watermark
        minWatermarkIndex = nextMinWatermarkIndex;
        currMinWatermark = nextMinWatermark;

        if (LOG.isDebugEnabled()) {
          LOG.debug("Emit watermark {}", currMinWatermark);
        }

          /*
          if (taskId.startsWith("Stage1")) {
            LOG.info("Emit watermark {} in {} for {}", new Instant(currMinWatermark.getTimestamp()), taskId, vertex.getId());
          }
          */

        //LOG.info("Emit watermark multiManager {}/{}, {}", vertex.getId(),
        //  taskId,
        //  new Instant(currMinWatermark.getTimestamp()));
        watermarkCollector.emitWatermark(currMinWatermark);
      }
    } else {
      // The recent watermark timestamp cannot be less than the previous one
      // because watermark is monotonically increasing.
      if (watermarks.get(edgeIndex).getTimestamp() > watermark.getTimestamp()) {
        LOG.warn(
          "The recent watermark timestamp cannot be less than the previous one "
            + "because watermark is monotonically increasing... " +
            "TaskId: " + taskId + " srcIndex: " + edgeIndex + ", prevWatermark: " +
            new Instant(watermarks.get(edgeIndex).getTimestamp()) + ", currWatermark: " +
        new Instant(watermark.getTimestamp()));
      } else {
        watermarks.set(edgeIndex, watermark);
      }
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
