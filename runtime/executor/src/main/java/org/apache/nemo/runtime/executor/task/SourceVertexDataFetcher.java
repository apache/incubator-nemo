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
package org.apache.nemo.runtime.executor.task;

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.punctuation.Finishmark;
import org.apache.nemo.common.punctuation.Latencymark;
import org.apache.nemo.common.punctuation.Watermark;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Fetches data from a data source.
 */
class SourceVertexDataFetcher extends DataFetcher {
  private final Readable readable;
  private final String taskId;
  private long boundedSourceReadTime = 0;
  private static final long WATERMARK_PERIOD = 1000; // ms
  private static final long LATENCYMARK_PERIOD = 1000; // ms
  private final ScheduledExecutorService streamMarkTriggerService;
  private boolean watermarkTriggered = false;
  private boolean latencyMarkTriggered = false;
  private final boolean bounded;

  SourceVertexDataFetcher(final SourceVertex dataSource,
                          final Readable readable,
                          final OutputCollector outputCollector,
                          final long latencyMarkSendPeriod,
                          final String taskId) {
    super(dataSource, outputCollector);
    this.taskId = taskId;
    this.readable = readable;
    this.readable.prepare();
    this.bounded = dataSource.isBounded();

    if (!bounded) {
      this.streamMarkTriggerService = Executors.newScheduledThreadPool(1);
      this.streamMarkTriggerService.scheduleAtFixedRate(() ->
        watermarkTriggered = true,
        WATERMARK_PERIOD, WATERMARK_PERIOD, TimeUnit.MILLISECONDS);
      if (latencyMarkSendPeriod != -1) {
        this.streamMarkTriggerService.scheduleAtFixedRate(() ->
            latencyMarkTriggered = true,
          latencyMarkSendPeriod, latencyMarkSendPeriod, TimeUnit.MILLISECONDS);
      }
    } else {
      this.streamMarkTriggerService = null;
    }
  }

  /**
   * This is non-blocking operation.
   *
   * @return current data
   */
  @Override
  Object fetchDataElement() {
    if (readable.isFinished()) {
      return Finishmark.getInstance();
    } else {
      final long start = System.currentTimeMillis();
      final Object element = retrieveElement();
      boundedSourceReadTime += System.currentTimeMillis() - start;
      return element;
    }
  }

  final long getBoundedSourceReadTime() {
    return boundedSourceReadTime;
  }

  @Override
  public void close() throws Exception {
    readable.close();
    if (streamMarkTriggerService != null) {
      streamMarkTriggerService.shutdown();
    }
  }

  private boolean isWatermarkTriggerTime() {
    if (watermarkTriggered) {
      watermarkTriggered = false;
      return true;
    } else {
      return false;
    }
  }

  private boolean isLatencyMarkTriggered() {
    if (latencyMarkTriggered) {
      latencyMarkTriggered = false;
      return true;
    } else {
      return false;
    }
  }

  private Object retrieveElement() {
    // Emit watermark
    if (!bounded) {
      if (isWatermarkTriggerTime()) {
        return new Watermark(readable.readWatermark());
      } else if (isLatencyMarkTriggered()) {
        return new Latencymark(taskId, System.currentTimeMillis());
      }
    }

    // Data
    return readable.readCurrent();
  }
}
