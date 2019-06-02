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
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.common.punctuation.Finishmark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.*;

/**
 * Fetches data from a data source.
 */
public class SourceVertexDataFetcher extends DataFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(SourceVertexDataFetcher.class.getName());

  private Readable readable;
  private long boundedSourceReadTime = 0;
  private static final long WATERMARK_PERIOD = 1000; // ms
  private final ScheduledExecutorService watermarkTriggerService;
  private boolean watermarkTriggered = false;
  private final boolean bounded;

  private boolean isStarted = false;

  private long prevWatermarkTimestamp = -1L;

  private volatile boolean isFinishd = false;
  //private volatile boolean finishedAck = false;

  private boolean isPrepared = false;

  private final ExecutorService prepareService;

  public SourceVertexDataFetcher(final SourceVertex dataSource,
                                 final RuntimeEdge edge,
                                 final Readable readable,
                                 final OutputCollector outputCollector,
                                 final ExecutorService prepareService) {
    super(dataSource, edge, outputCollector);
    this.readable = readable;
    this.bounded = dataSource.isBounded();
    this.prepareService = prepareService;

    LOG.info("Is bounded: {}, source: {}", bounded, dataSource);
    if (!bounded) {
      this.watermarkTriggerService = Executors.newScheduledThreadPool(1);
      this.watermarkTriggerService.scheduleAtFixedRate(() -> {
        watermarkTriggered = true;
      }, WATERMARK_PERIOD, WATERMARK_PERIOD, TimeUnit.MILLISECONDS);
    } else {
      this.watermarkTriggerService = null;
    }
  }

  public void setReadable(final Readable r) {
    readable = r;
    isPrepared = false;
    isStarted = false;
  }

  public boolean isStarted() {
    return isStarted;
  }

  public Readable getReadable() {
    return readable;
  }

  /**
   * This is non-blocking operation.
   * @return current data
   * @throws NoSuchElementException if the current data is not available
   */
  @Override
  public Object fetchDataElement() throws NoSuchElementException, IOException {
    if (isFinishd) {
      //finishedAck = true;
      LOG.info("Fetch data element after isFinished set");
      throw new NoSuchElementException();
    }

    if (!isStarted) {
      isStarted = true;
      LOG.info("Readable: {}", readable);
      prepareService.execute(() -> {
        this.readable.prepare();
        isPrepared = true;
      });
    }

    if (!isPrepared) {
      LOG.info("Not prepared... ");
      throw new NoSuchElementException();
    }

    if (readable.isFinished()) {
      return Finishmark.getInstance();
    } else {
      final long start = System.currentTimeMillis();
      final Object element = retrieveElement();
      boundedSourceReadTime += System.currentTimeMillis() - start;
      return element;
    }
  }

  @Override
  public Future<Integer> stop() {
    isFinishd = true;
    // do nothing
    return new Future<Integer>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return true;
      }

      @Override
      public Integer get() throws InterruptedException, ExecutionException {
        return 1;
      }

      @Override
      public Integer get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return 1;
      }
    };
  }

  @Override
  public void restart() {
    //finishedAck = false;
    isFinishd = false;
  }

  public final long getBoundedSourceReadTime() {
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
      final long watermarkTimestamp = readable.readWatermark();
      if (prevWatermarkTimestamp < watermarkTimestamp) {
        prevWatermarkTimestamp = watermarkTimestamp;
        return new Watermark(watermarkTimestamp);
      }
    }

    // Data
    final Object element = readable.readCurrent();
    return element;
  }
}
