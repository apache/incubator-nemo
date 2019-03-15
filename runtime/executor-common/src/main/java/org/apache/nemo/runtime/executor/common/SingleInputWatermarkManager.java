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

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.punctuation.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.PriorityQueue;

/**
 * This is a special implementation for single input data stream for optimization.
 */
public final class SingleInputWatermarkManager implements InputWatermarkManager {
  private static final Logger LOG = LoggerFactory.getLogger(SingleInputWatermarkManager.class.getName());

  private final OutputCollector watermarkCollector;
  private String sourceId;
  final Map<String, Pair<PriorityQueue<Watermark>, PriorityQueue<Watermark>>> expectedWatermarkMap;
  private final IRVertex irVertex;
  private final Map<Long, Long> prevWatermarkMap;
  private final Map<Long, Integer> watermarkCounterMap;

  public SingleInputWatermarkManager(final OutputCollector watermarkCollector,
                                     final IRVertex irVertex,
                                     final Map<String, Pair<PriorityQueue<Watermark>, PriorityQueue<Watermark>>> expectedWatermarkMap,
                                     final Map<Long, Long> prevWatermarkMap,
                                     final Map<Long, Integer> watermarkCounterMap) {
    this.watermarkCollector = watermarkCollector;
    this.expectedWatermarkMap = expectedWatermarkMap;
    this.irVertex = irVertex;
    this.prevWatermarkMap = prevWatermarkMap;
    this.watermarkCounterMap = watermarkCounterMap;
  }

  /**
   * This just forwards watermarks to the next operator because it has one data stream.
   * @param edgeIndex edge index
   * @param watermark watermark
   */
  @Override
  public void trackAndEmitWatermarks(final int edgeIndex,
                                     final Watermark watermark) {
    if (expectedWatermarkMap == null) {
      watermarkCollector.emitWatermark(watermark);
    } else {
      final PriorityQueue<Watermark> expectedWatermarkQueue = expectedWatermarkMap.get(irVertex.getId()).left();
      if (!expectedWatermarkQueue.isEmpty()) {
        // FOR OFFLOADING

        //LOG.info("Expected min: {}, Curr watermark: {} at {}", expectedWatermarkQueue.peek(),
        //  watermark, irVertex.getId());

        // we should not emit the watermark directly.
        final PriorityQueue<Watermark> pendingWatermarkQueue = expectedWatermarkMap.get(irVertex.getId()).right();
        pendingWatermarkQueue.add(watermark);
        while (!expectedWatermarkQueue.isEmpty() && !pendingWatermarkQueue.isEmpty() &&
          expectedWatermarkQueue.peek().getTimestamp() >= pendingWatermarkQueue.peek().getTimestamp()) {

           LOG.info("Expected min: {}, Pending  watermark: {} at {}", expectedWatermarkQueue.peek(),
          pendingWatermarkQueue.peek(), irVertex.getId());

          // check whether outputs are emitted
          final long ts = pendingWatermarkQueue.peek().getTimestamp();
          if (expectedWatermarkQueue.peek().getTimestamp() > ts) {
            LOG.warn("This may be emitted from the internal {}: {}, we don't have to emit it again", irVertex.getId(), ts);
            pendingWatermarkQueue.poll();
            //LOG.info("Emit watermark {} at {} by processing offloading watermark",
            //  watermarkToBeEmitted, irVertex.getId());
            //watermarkCollector.emitWatermark(watermarkToBeEmitted);
          } else {
            if (!prevWatermarkMap.containsKey(ts)) {
              LOG.warn("This may be deleted of prev watermark: {}", ts);
              final Watermark watermarkToBeEmitted = expectedWatermarkQueue.poll();
              pendingWatermarkQueue.poll();
              LOG.info("Emit watermark {} at {} by processing offloading watermark",
                watermarkToBeEmitted, irVertex.getId());
              watermarkCollector.emitWatermark(watermarkToBeEmitted);
            } else {
              final long prevWatermark = prevWatermarkMap.get(ts);
              if (watermarkCounterMap.getOrDefault(prevWatermark, 0) == 0) {
                LOG.info("Remove {}  prev watermark: {}", ts, prevWatermark);
                final Watermark watermarkToBeEmitted = expectedWatermarkQueue.poll();
                pendingWatermarkQueue.poll();
                prevWatermarkMap.remove(prevWatermark);
                watermarkCounterMap.remove(prevWatermark);

                LOG.info("Emit watermark {} at {} by processing offloading watermark",
                  watermarkToBeEmitted, irVertex.getId());
                watermarkCollector.emitWatermark(watermarkToBeEmitted);
              } else {
                // We should wait for other outputs
                break;
              }
            }
          }
        }
      } else {
        watermarkCollector.emitWatermark(watermark);
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
