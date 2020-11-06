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

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.punctuation.Finishmark;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.runtime.executor.bytetransfer.LocalOutputContext;
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.apache.nemo.runtime.executor.datatransfer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Task thread -> fetchDataElement() -> (((QUEUE))) <- List of iterators <- queueInsertionThreads
 * <p>
 * Unlike {@link ParentTaskDataFetcher}, where the task thread directly consumes (and blocks on) iterators one by one,
 * this class spawns threads that each forwards elements from an iterator to a global queue.
 * <p>
 * This class should be used when dealing with unbounded data streams, as we do not want to be blocked on a
 * single unbounded iterator forever.
 */
@NotThreadSafe
class MultiThreadParentTaskDataFetcher extends DataFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(MultiThreadParentTaskDataFetcher.class);

  private final PipeInputReader readersForParentTask;
  private final ExecutorService queueInsertionThreads;

  // Non-finals (lazy fetching)
  private boolean firstFetch = true;


  // elements
  private final ConcurrentLinkedQueue elementQueue;

  private long serBytes = 0;
  private long encodedBytes = 0;

  private int numOfIterators; // == numOfIncomingEdges
  private int numOfFinishMarks = 0;

  // A watermark manager
  private InputWatermarkManager inputWatermarkManager;


  MultiThreadParentTaskDataFetcher(final IRVertex dataSource,
                                   final PipeInputReader readerForParentTask,
                                   final OutputCollector outputCollector) {
    super(dataSource, outputCollector);
    this.readersForParentTask = readerForParentTask;
    this.firstFetch = true;
    this.elementQueue = new ConcurrentLinkedQueue();
    this.queueInsertionThreads = Executors.newCachedThreadPool();
  }

  @Override
  Object fetchDataElement() throws IOException {
    if (firstFetch) {
      LOG.error("{} : checkpoint 6", Thread.currentThread());
      fetchDataLazily();
      firstFetch = false;
    }

    while (true) {
      final Object element = elementQueue.poll();
      if (element == null) {
        throw new NoSuchElementException();
      } else if (element instanceof Finishmark) {
        numOfFinishMarks++;
        if (numOfFinishMarks == numOfIterators) {
          return Finishmark.getInstance();
        }
        // else try again.
      } else {
        return element;
      }
    }
  }

  private void fetchDataLazily() {
    // should be fixed. Read from local and remote
    final List<CompletableFuture<Object>> futures = readersForParentTask.read1();

    numOfIterators = futures.size();

    // Read from local and remote -> Pair<List<CompetableFuture<DataUtil.IteratorWithn>, List<LocalOutputContext>>

    if (numOfIterators > 1) {
      LOG.error("single watermark manager created ");
      inputWatermarkManager = new MultiInputWatermarkManager(numOfIterators, new WatermarkCollector());
    } else {
      LOG.error("multiple watermark amnager creatd");
      inputWatermarkManager = new SingleInputWatermarkManager(new WatermarkCollector());
    }

    /**
    for (CompletableFuture<DataUtil.IteratorWithNumBytes> compFuture : externals) {
      compFuture.whenComplete((iterator, exception) -> queueInsertionThreads.submit(() -> {
        if (exception == null) {
          // Consume this iterator to the end.
          while (iterator.hasNext()) { // blocked on the iterator.
            final Object element = iterator.next();
            if (element instanceof WatermarkWithIndex) {
              // watermark element
              // the input watermark manager is accessed by multiple threads
              // so we should synchronize it
              synchronized (inputWatermarkManager) {
                final WatermarkWithIndex watermarkWithIndex = (WatermarkWithIndex) element;
                inputWatermarkManager.trackAndEmitWatermarks(
                  watermarkWithIndex.getIndex(), watermarkWithIndex.getWatermark());
              }
            } else {
              // data element
              LOG.error("offering element*** : {}", element);
              elementQueue.offer(element);
            }
          }
          // This iterator is finished.
          countBytesSynchronized(iterator);
          elementQueue.offer(Finishmark.getInstance());
        } else {
          LOG.error(exception.getMessage());
          throw new RuntimeException(exception);
        }
      }));
    }
    */


    // Adding elements from remote executor into queue
    futures.forEach(compFuture -> compFuture.whenComplete((iterator, exception) ->
      // A thread for each iterator
    {
      if (iterator instanceof DataUtil.IteratorWithNumBytes) {
        queueInsertionThreads.submit(() -> {
          DataUtil.IteratorWithNumBytes iter = (DataUtil.IteratorWithNumBytes) iterator;
          if (exception == null) {
            // Consume this iterator to the end.
            while (iter.hasNext()) { // blocked on the iterator.
              final Object element = iter.next();
              if (element instanceof WatermarkWithIndex) {
                // watermark element
                // the input watermark manager is accessed by multiple threads
                // so we should synchronize it
                synchronized (inputWatermarkManager) {
                  final WatermarkWithIndex watermarkWithIndex = (WatermarkWithIndex) element;
                  inputWatermarkManager.trackAndEmitWatermarks(
                    watermarkWithIndex.getIndex(), watermarkWithIndex.getWatermark());
                }
              } else {
                // data element
                LOG.error("offering element*** : {}", element);
                elementQueue.offer(element);
              }
            }

            // This iterator is finished.
            countBytesSynchronized(iter);
            LOG.error("offering element finishmark");
            elementQueue.offer(Finishmark.getInstance());
          } else {
            LOG.error(exception.getMessage());
            throw new RuntimeException(exception);
          }
        });
      }
      else {
        queueInsertionThreads.submit(() -> {
          LocalOutputContext localOutputContext = (LocalOutputContext) iterator;
          boolean isfinished = false;
          while (!isfinished) { // blocked on the iterator.
            final Object element = localOutputContext.read();
            if (element == null) {
              continue;
            }
            if (element instanceof WatermarkWithIndex && ((WatermarkWithIndex) element).getWatermark().getTimestamp() == Long.MAX_VALUE) {
              isfinished = true;
              LOG.error("max watermark received");
            }
            if (element instanceof Finishmark) isfinished = true;
            if (element instanceof WatermarkWithIndex) {
              LOG.error("Watermark timestamp : {}", ((WatermarkWithIndex) element).getWatermark().getTimestamp());
              if (((WatermarkWithIndex) element).getWatermark().compareTo(new Watermark(9223372036854775L)) == 0) {
                isfinished = true;
                WatermarkWithIndex a = new WatermarkWithIndex(new Watermark(Long.MAX_VALUE), 1);
                LOG.error("experiment : timestamp : {}", a.getWatermark().getTimestamp());
                LOG.error("experiment : {}", a.getWatermark().compareTo(new Watermark(9223372036854775L)));
                LOG.error("max watermark received");
              }


              // watermark element
              // the input watermark manager is accessed by multiple threads
              // so we should synchronize it
              synchronized (inputWatermarkManager) {
                final WatermarkWithIndex watermarkWithIndex = (WatermarkWithIndex) element;
                inputWatermarkManager.trackAndEmitWatermarks(
                  watermarkWithIndex.getIndex(), watermarkWithIndex.getWatermark());
              }
            } else {
              // data element
              LOG.error("From local executor, offering element*** : {}", element);
              elementQueue.offer(element);
            }
          }
          LOG.error("offering finishamark");
          elementQueue.offer(Finishmark.getInstance());
        });
      }
    }
    ));

    /**
    // Adding elements from local executor into queue
    for (LocalOutputContext local : locals) {
      queueInsertionThreads.submit(() -> {
        boolean isfinished = false;
        while (!isfinished) { // blocked on the iterator.
          final Object element = local.read();
          if (element == null) {
            try {
              Thread.sleep(100);
            }
            catch (InterruptedException e) {
              e.printStackTrace();
            }
            continue;
          }
          if (element instanceof Finishmark) isfinished = true;
          if (element instanceof WatermarkWithIndex) {
            // watermark element
            // the input watermark manager is accessed by multiple threads
            // so we should synchronize it
            synchronized (inputWatermarkManager) {
              final WatermarkWithIndex watermarkWithIndex = (WatermarkWithIndex) element;
              inputWatermarkManager.trackAndEmitWatermarks(
                watermarkWithIndex.getIndex(), watermarkWithIndex.getWatermark());
            }
          } else {
            // data element
            LOG.error("From local executor, offering element*** : {}", element);
            elementQueue.offer(element);
          }
          LOG.error("loop ending");
        }
      });
    }
     */

    /**
    locals.forEach(localOutputContext ->
      queueInsertionThreads.submit(() -> {
        boolean isfinished = false;
        while (!isfinished) { // blocked on the iterator.
          final Object element = localOutputContext.read();
          if (element == null) {
            try {
              Thread.sleep(100);
            }
            catch (InterruptedException e) {
              e.printStackTrace();
            }
            continue;
          }
          if (element instanceof Finishmark) isfinished = true;
          if (element instanceof WatermarkWithIndex) {
            // watermark element
            // the input watermark manager is accessed by multiple threads
            // so we should synchronize it
            synchronized (inputWatermarkManager) {
              final WatermarkWithIndex watermarkWithIndex = (WatermarkWithIndex) element;
              inputWatermarkManager.trackAndEmitWatermarks(
                watermarkWithIndex.getIndex(), watermarkWithIndex.getWatermark());
            }
          } else {
            // data element
            LOG.error("From local executor, offering element*** : {}", element);
            elementQueue.offer(element);
          }
          LOG.error("loop ending");
        }
      }));
     */
  }

  final long getSerializedBytes() {
    return serBytes;
  }

  final long getEncodedBytes() {
    return encodedBytes;
  }

  private synchronized void countBytesSynchronized(final DataUtil.IteratorWithNumBytes iterator) {
    try {
      serBytes += iterator.getNumSerializedBytes();
    } catch (final DataUtil.IteratorWithNumBytes.NumBytesNotSupportedException e) {
      serBytes = -1;
    } catch (final IllegalStateException e) {
      LOG.error("Failed to get the number of bytes of serialized data - the data is not ready yet ", e);
    }
    try {
      encodedBytes += iterator.getNumEncodedBytes();
    } catch (final DataUtil.IteratorWithNumBytes.NumBytesNotSupportedException e) {
      encodedBytes = -1;
    } catch (final IllegalStateException e) {
      LOG.error("Failed to get the number of bytes of encoded data - the data is not ready yet ", e);
    }
  }

  @Override
  public void close() throws Exception {
    queueInsertionThreads.shutdown();
  }

  /**
   * Just adds the emitted watermark to the element queue.
   * It receives the watermark from InputWatermarkManager.
   */
  private final class WatermarkCollector implements OutputCollector {
    @Override
    public void emit(final Object output) {
      throw new IllegalStateException("Should not be called");
    }

    @Override
    public void emitWatermark(final Watermark watermark) {
      elementQueue.offer(watermark);
    }

    @Override
    public void emit(final String dstVertexId, final Object output) {
      throw new IllegalStateException("Should not be called");
    }
  }
}
