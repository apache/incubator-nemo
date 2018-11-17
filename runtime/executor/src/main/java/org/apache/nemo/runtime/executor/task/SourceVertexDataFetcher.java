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
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.common.punctuation.Finishmark;
import org.apache.nemo.runtime.executor.datatransfer.InputWatermarkManager;
import org.apache.nemo.runtime.executor.datatransfer.WatermarkWithIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Fetches data from a data source.
 */
class SourceVertexDataFetcher extends DataFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(SourceVertexDataFetcher.class.getName());

  private final Readable readable;
  private long boundedSourceReadTime = 0;
  private static final long WATERMARK_PERIOD = 1000; // ms
  private final ScheduledExecutorService watermarkTriggerService;
  private boolean watermarkTriggered = false;
  private final boolean bounded;

  SourceVertexDataFetcher(final SourceVertex dataSource,
                          final Readable readable,
                          final OutputCollector outputCollector,
                          final InputWatermarkManager inputWatermarkManager) {
    super(dataSource, outputCollector, inputWatermarkManager);
    this.readable = readable;
    this.readable.prepare();
    this.bounded = dataSource.isBounded();

    if (!bounded) {
      this.watermarkTriggerService = Executors.newScheduledThreadPool(1);
      this.watermarkTriggerService.scheduleAtFixedRate(() -> {
        watermarkTriggered = true;
      }, WATERMARK_PERIOD, WATERMARK_PERIOD, TimeUnit.MILLISECONDS);
    } else {
      this.watermarkTriggerService = null;
    }
  }

  /**
   * This is non-blocking operation.
   * @return current data
   * @throws NoSuchElementException if the current data is not available
   */
  @Override
  Object fetchDataElement() throws NoSuchElementException, IOException {
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
    if (watermarkTriggerService != null) {
      watermarkTriggerService.shutdown();
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

  private Object retrieveElement() throws NoSuchElementException, IOException {
    // Emit watermark
    if (!bounded && isWatermarkTriggerTime()) {
      // index=0 as there is only 1 input stream
      return new WatermarkWithIndex(new Watermark(readable.readWatermark()), 0);
    }

    // Data
    final Object element = readable.readCurrent();
    return element;
  }
}
