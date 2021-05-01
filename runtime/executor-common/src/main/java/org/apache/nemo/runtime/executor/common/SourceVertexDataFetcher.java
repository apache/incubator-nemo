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

import org.apache.nemo.common.Util;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.punctuation.EmptyElement;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.common.punctuation.Finishmark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Fetches data from a data source.
 */
public class SourceVertexDataFetcher extends DataFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(SourceVertexDataFetcher.class.getName());

  private volatile Readable readable;
  private long boundedSourceReadTime = 0;
  private static final long WATERMARK_PROGRESS = Util.WATERMARK_PROGRESS; // ms
  private final boolean bounded;

  private boolean isStarted = false;

  private long prevWatermarkTimestamp = -1L;

  private volatile boolean isFinishd = false;
  //private volatile boolean finishedAck = false;

  private volatile boolean isPrepared = false;

  private final ExecutorService prepareService;

  private final String taskId;


  private boolean watermarkProgressed = false;

  private final AtomicBoolean globalPrepared;

  private final boolean offloaded;

  public SourceVertexDataFetcher(final SourceVertex dataSource,
                                 final RuntimeEdge edge,
                                 final Readable r,
                                 final OutputCollector outputCollector,
                                 final ExecutorService prepareService,
                                 final String taskId,
                                 final AtomicBoolean globalPreapred,
                                 final Readable.ReadableContext readableContext,
                                 final boolean offloaded) {
    super(dataSource, edge, outputCollector);
    this.readable = r;
    this.globalPrepared = globalPreapred;
    this.bounded = dataSource.isBounded();
    this.prepareService = prepareService;
    this.taskId = taskId;
    this.prevWatermarkTimestamp = 0;
    this.offloaded = offloaded;

    if (!isStarted && readable != null && !offloaded) {
      isStarted = true;
      LOG.info("Reset readable: {} for {}", readable, taskId);
      prepareService.execute(() -> {
        this.readable.prepare(readableContext);
        LOG.info("Prepare finished {}", taskId);
        isPrepared = true;
      });
    }


    /*
    if (!bounded) {
      this.executorGlobalInstances.registerWatermarkService(dataSource, () -> {
        if (isPrepared && globalPreapred.get()) {
          final long watermarkTimestamp = readable.readWatermark();
         //LOG.info("prev watermark: {}, Curr watermark: {}", prevWatermarkTimestamp, watermarkTimestamp);
          if (prevWatermarkTimestamp + WATERMARK_PROGRESS <= watermarkTimestamp) {
           // LOG.info("Watermark progressed at {} {}", taskId, watermarkTimestamp);
            prevWatermarkTimestamp = watermarkTimestamp;
            watermarkProgressed = true;
          }
        }
      });
    }
    */
  }

  /*
  public void setReadable(final Readable r) {
    LOG.info("Set readable for {}", taskId);
    readable = r;
    isPrepared = false;
    isStarted = false;
  }
  */

  @Override
  public void prepare() {

  }

  public long getPrevWatermarkTimestamp() {
    return prevWatermarkTimestamp;
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
  public Object fetchDataElement() {
    if (isFinishd) {
      //finishedAck = true;
      LOG.info("Fetch data element after isInputFinished set for {}", taskId);
      return EmptyElement.getInstance();
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
  public Future<Integer> stop(final String taskId) {
    LOG.info("IsFinished true for {}", taskId);
    isFinishd = true;
    // executorGlobalInstances.deregisterWatermarkService((SourceVertex) getDataSource());
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
    /*
    executorGlobalInstances.registerWatermarkService((SourceVertex) getDataSource(), () -> {
      if (isPrepared && globalPrepared.get()) {
        final long watermarkTimestamp = readable.readWatermark();
        if (prevWatermarkTimestamp + WATERMARK_PROGRESS <= watermarkTimestamp) {
          watermarkProgressed = true;
          prevWatermarkTimestamp = watermarkTimestamp;
        }
      }
    });
    */
    //finishedAck = false;
    isFinishd = false;
  }

  public final long getBoundedSourceReadTime() {
    return boundedSourceReadTime;
  }

  @Override
  public void close() throws Exception {
    // executorGlobalInstances.deregisterWatermarkService((SourceVertex) getDataSource());
    readable.close();
  }


  @Override
  public boolean isAvailable() {
    //LOG.info("Source {} available: {}, {}, {}, {}",
    //  taskId, isStarted, isFinishd, readable.isAvailable(), watermarkProgressed);
    if (!isPrepared) {
      return false;
    }

    return isPrepared && !isFinishd;
  }

  @Override
  public boolean hasData() {
    return !isFinishd && readable.isAvailable() ||
      ((System.currentTimeMillis() - prevWatermarkTimestamp) >= WATERMARK_PROGRESS);
  }

  private Object retrieveElement() {
    // Emit watermark
    final long watermarkTimestamp = readable.readWatermark();
    // LOG.info("prev watermark: {}, Curr watermark: {}", prevWatermarkTimestamp, watermarkTimestamp);
    if (prevWatermarkTimestamp + WATERMARK_PROGRESS <= watermarkTimestamp) {
      // LOG.info("Watermark progressed at {} {}", taskId, watermarkTimestamp);
      prevWatermarkTimestamp = watermarkTimestamp;
      watermarkProgressed = true;
      return new Watermark(prevWatermarkTimestamp);
    } else {
      // Data
      final Object element = readable.readCurrent();
      return element;
    }
    /*
    if (watermarkProgressed) {
      watermarkProgressed = false;
    }
    */
  }
}
