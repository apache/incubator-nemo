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

import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.common.ir.AbstractOutputCollector;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.punctuation.Finishmark;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.apache.nemo.runtime.executor.datatransfer.InputReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.*;

/**
 * Task thread -> fetchDataElement() -> (((QUEUE))) <- List of iterators <- queueInsertionThreads
 *
 * Unlike {@link ParentTaskDataFetcher}, where the task thread directly consumes (and blocks on) iterators one by one,
 * this class spawns threads that each forwards elements from an iterator to a global queue.
 *
 * This class should be used when dealing with unbounded data streams, as we do not want to be blocked on a
 * single unbounded iterator forever.
 */
@NotThreadSafe
class MultiThreadParentTaskDataFetcher extends DataFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(MultiThreadParentTaskDataFetcher.class);

  private final InputReader readersForParentTask;
  //private final ExecutorService queueInsertionThreads;

  // Non-finals (lazy fetching)
  private boolean firstFetch = true;

  private final ConcurrentLinkedQueue<Watermark> watermarkQueue;

  private long serBytes = 0;
  private long encodedBytes = 0;

  private int numOfIterators; // == numOfIncomingEdges
  private int numOfFinishMarks = 0;

  // A watermark manager
  private InputWatermarkManager inputWatermarkManager;

  private final String taskId;

  private final List<DataUtil.IteratorWithNumBytes> iterators;

  private final List<DataUtil.IteratorWithNumBytes> newIterators;

  private int iteratorIndex = 0;

  private final ConcurrentMap<DataUtil.IteratorWithNumBytes, Integer> iteratorTaskIndexMap;
  private final ExecutorService queueInsertionThreads;
  private final ConcurrentLinkedQueue elementQueue;

  MultiThreadParentTaskDataFetcher(final String taskId,
                                   final IRVertex dataSource,
                                   final RuntimeEdge edge,
                                   final InputReader readerForParentTask,
                                   final OutputCollector outputCollector) {
    super(dataSource, edge, outputCollector);
    this.taskId = taskId;
    this.readersForParentTask = readerForParentTask;
    this.firstFetch = true;
    this.watermarkQueue = new ConcurrentLinkedQueue<>();
    this.elementQueue = new ConcurrentLinkedQueue();

    //this.queueInsertionThreads = Executors.newCachedThreadPool();
    this.iterators = new ArrayList<>();
    this.newIterators = new ArrayList<>();
    this.iteratorTaskIndexMap = new ConcurrentHashMap<>();
    this.queueInsertionThreads = Executors.newCachedThreadPool();

    fetchNonBlocking();
  }

  @Override
  public Object fetchDataElement() throws IOException, NoSuchElementException {
    if (firstFetch) {
      //fetchDataLazily();
      firstFetch = false;
    }

    if (!newIterators.isEmpty()) {
      LOG.info("Add new iterators {}", newIterators.size());
      iterators.addAll(newIterators);
      synchronized (newIterators) {
        newIterators.clear();
      }
    }


    if (!watermarkQueue.isEmpty()) {
      return watermarkQueue.poll();
    }

    int cnt = 0;
    while (cnt < iterators.size()) {
      final DataUtil.IteratorWithNumBytes iterator = iterators.get(iteratorIndex);

      if (iterator.isFinished()) {

        final DynamicInputWatermarkManager watermarkManager = (DynamicInputWatermarkManager) inputWatermarkManager;
        iterators.remove(iteratorIndex);
        final Integer taskIndex = iteratorTaskIndexMap.remove(iterator);
        LOG.info("Task index {} finished at {}", taskIndex);
        watermarkManager.removeEdge(taskIndex);
        iteratorIndex = iterators.size() == 0 ? 0 : iteratorIndex % iterators.size();

      } else if (iterator.hasNext()) {
        iteratorIndex = (iteratorIndex + 1) % iterators.size();
        final Object element = iterator.next();

        if (element instanceof WatermarkWithIndex) {
          // watermark element
          // the input watermark manager is accessed by multiple threads
          // so we should synchronize it
          final WatermarkWithIndex watermarkWithIndex = (WatermarkWithIndex) element;
          inputWatermarkManager.trackAndEmitWatermarks(
            watermarkWithIndex.getIndex(), watermarkWithIndex.getWatermark());
        } else {
          // data element
          return element;
        }
      } else {
        iteratorIndex = (iteratorIndex + 1) % iterators.size();
      }

      cnt += 1;
    }

    throw new NoSuchElementException();
  }

  private void fetchNonBlocking() { // 갯수 동적으로 받아야함. handler 같은거 등록하기
    inputWatermarkManager = new DynamicInputWatermarkManager(taskId, getDataSource(), new WatermarkCollector());
    final DynamicInputWatermarkManager watermarkManager = (DynamicInputWatermarkManager) inputWatermarkManager;

    readersForParentTask.readAsync(pair -> {

      LOG.info("Receive iterator task {} at {} edge {}"
        , pair.right(), readersForParentTask.getTaskIndex(), edge.getId());
      final DataUtil.IteratorWithNumBytes iterator = pair.left();
      final int taskIndex = pair.right();
      iteratorTaskIndexMap.put(iterator, taskIndex);
      watermarkManager.addEdge(taskIndex);

      synchronized (newIterators) {
        newIterators.add(iterator);
      }
    });
  }


  private void fetchAsync() { // 갯수 동적으로 받아야함. handler 같은거 등록하기

    inputWatermarkManager = new DynamicInputWatermarkManager(taskId, getDataSource(), new WatermarkCollector());
    final DynamicInputWatermarkManager watermarkManager = (DynamicInputWatermarkManager) inputWatermarkManager;

    readersForParentTask.readAsync(pair -> {
      LOG.info("Receive iterator task {} at {} edge {}"
        , pair.right(), readersForParentTask.getTaskIndex(), edge.getId());
      final DataUtil.IteratorWithNumBytes iterator = pair.left();
      final int taskIndex = pair.right();
      watermarkManager.addEdge(taskIndex);

      queueInsertionThreads.submit(() -> {
        // Consume this iterator to the end.
        while (iterator.hasNext()) {
          // blocked on the iterator.
          final Object element = iterator.next();
          if (element instanceof WatermarkWithIndex) {
            // watermark element
            // the input watermark manager is accessed by multiple threads
            // so we should synchronize it
            final WatermarkWithIndex watermarkWithIndex = (WatermarkWithIndex) element;
            inputWatermarkManager.trackAndEmitWatermarks(
              watermarkWithIndex.getIndex(), watermarkWithIndex.getWatermark());
          } else {
            // data element
            elementQueue.offer(element);
          }
        }

        // This iterator is finished.
        LOG.info("Task index {} finished at {}", taskIndex);
        watermarkManager.removeEdge(taskIndex);
      });
    });
  }

  private void fetchBlocking() {
    // 갯수 동적으로 받아야함. handler 같은거 등록하기
    final List<DataUtil.IteratorWithNumBytes> inputs = readersForParentTask.readBlocking();
    numOfIterators = inputs.size();

    if (numOfIterators > 1) {
      inputWatermarkManager = new MultiInputWatermarkManager(getDataSource(), numOfIterators, new WatermarkCollector());
    } else {
      inputWatermarkManager = new SingleInputWatermarkManager(
        new WatermarkCollector(), null, null, null, null);
    }

    for (final DataUtil.IteratorWithNumBytes iterator : inputs) {
      // A thread for each iterator
      queueInsertionThreads.submit(() -> {
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
            elementQueue.offer(element);
          }
        }

        // This iterator is finished.
        countBytesSynchronized(iterator);
        elementQueue.offer(Finishmark.getInstance());
      });
    }
  }


  private void fetchDataLazily() {
    final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = readersForParentTask.read();
    numOfIterators = futures.size();

    if (numOfIterators > 1) {
      inputWatermarkManager = new MultiInputWatermarkManager(getDataSource(), numOfIterators, new WatermarkCollector());
    } else {
      inputWatermarkManager = new SingleInputWatermarkManager(
        new WatermarkCollector(), null, null, null, null);
    }

    futures.forEach(compFuture -> compFuture.whenComplete((iterator, exception) -> {
      // A thread for each iterator
      queueInsertionThreads.submit(() -> {
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
              elementQueue.offer(element);
            }
          }

          // This iterator is finished.
          countBytesSynchronized(iterator);
          elementQueue.offer(Finishmark.getInstance());
        } else {
          exception.printStackTrace();
          throw new RuntimeException(exception);
        }
      });

    }));
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
  private final class WatermarkCollector extends AbstractOutputCollector {
    @Override
    public void emit(final Object output) {
      throw new IllegalStateException("Should not be called");
    }
    @Override
    public void emitWatermark(final Watermark watermark) {
      watermarkQueue.offer(watermark);
    }
    @Override
    public void emit(final String dstVertexId, final Object output) {
      throw new IllegalStateException("Should not be called");
    }
  }
}
