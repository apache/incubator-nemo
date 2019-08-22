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
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.punctuation.EmptyElement;
import org.apache.nemo.runtime.executor.common.*;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.edge.RuntimeEdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.punctuation.Finishmark;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.runtime.executor.common.datatransfer.IteratorWithNumBytes;
import org.apache.nemo.runtime.executor.common.datatransfer.InputReader;
import org.apache.nemo.runtime.lambdaexecutor.datatransfer.RendevousServerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.*;

/**
 * Task thread -> fetchDataElement() -> (((QUEUE))) <- List of iterators <- queueInsertionThreads
 *
 * Unlike, where the task thread directly consumes (and blocks on) iterators one by one,
 * this class spawns threads that each forwards elements from an iterator to a global queue.
 *
 * This class should be used when dealing with unbounded data streams, as we do not want to be blocked on a
 * single unbounded iterator forever.
 */
@NotThreadSafe
public final class MultiThreadParentTaskDataFetcher extends DataFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(MultiThreadParentTaskDataFetcher.class);

  private final InputReader readersForParentTask;
  //private final ExecutorService queueInsertionThreads;

  // Non-finals (lazy fetching)
  private boolean firstFetch = true;

  private long serBytes = 0;
  private long encodedBytes = 0;

  private int numOfIterators; // == numOfIncomingEdges
  private int numOfFinishMarks = 0;

  // A watermark manager

  private final String taskId;

  private final List<IteratorWithNumBytes> iterators;

  private int iteratorIndex = 0;

  private final ConcurrentMap<IteratorWithNumBytes, Integer> iteratorTaskIndexMap;
  private final ConcurrentMap<Integer, IteratorWithNumBytes> taskIndexIteratorMap;

  private final ExecutorService queueInsertionThreads;
  private final ConcurrentLinkedQueue elementQueue;

  private final ConcurrentLinkedQueue<Pair<IteratorWithNumBytes, Integer>> taskAddPairQueue;

  private final RendevousServerClient rendevousServerClient;

  private final String stageId;

  private final ExecutorGlobalInstances executorGlobalInstances;
  private volatile boolean watermarkTriggered = false;

  private long prevWatermarkTimestamp = -1L;

  private final TaskExecutor taskExecutor;

  public MultiThreadParentTaskDataFetcher(final String taskId,
                                          final IRVertex dataSource,
                                          final RuntimeEdge edge,
                                          final InputReader readerForParentTask,
                                          final OutputCollector outputCollector,
                                          final RendevousServerClient rendevousServerClient,
                                          final ExecutorGlobalInstances executorGlobalInstances,
                                          final TaskExecutor taskExecutor) {
    super(dataSource, edge, outputCollector);

    readerForParentTask.setDataFetcher(this);

    this.taskId = taskId;
    this.stageId = RuntimeIdManager.getStageIdFromTaskId(taskId);
    this.readersForParentTask = readerForParentTask;
    this.firstFetch = true;
    this.elementQueue = new ConcurrentLinkedQueue();
    this.rendevousServerClient = rendevousServerClient;

    //this.queueInsertionThreads = Executors.newCachedThreadPool();
    this.iterators = new ArrayList<>();
    this.iteratorTaskIndexMap = new ConcurrentHashMap<>();
    this.taskIndexIteratorMap = new ConcurrentHashMap<>();
    this.queueInsertionThreads = Executors.newCachedThreadPool();
    this.taskAddPairQueue = new ConcurrentLinkedQueue<>();
    this.taskExecutor = taskExecutor;

    fetchNonBlocking();

    this.executorGlobalInstances = executorGlobalInstances;
    executorGlobalInstances.registerWatermarkService(dataSource, () -> {
      final Optional<Long> watermark = rendevousServerClient.requestWatermark(taskId);
      //LOG.info("Request watermark at {}", taskId);

      if (watermark.isPresent() && prevWatermarkTimestamp + Util.WATERMARK_PROGRESS <= watermark.get()) {
        //LOG.info("Receive watermark at {}: {}", taskId, new Instant(watermark.get()));
        prevWatermarkTimestamp = watermark.get();
        taskExecutor.handleIntermediateWatermarkEvent(new Watermark(watermark.get()), this);
      }
    });
  }

  @Override
  public boolean hasData() {
    boolean ret = false;
    for (final IteratorWithNumBytes iteratorWithNumBytes : iterators) {
      if (iteratorWithNumBytes.hasNext()) {
        ret = true;
        break;
      }
    }
    return ret;
  }

  @Override
  public boolean isAvailable() {
    boolean ret = false;
    for (final IteratorWithNumBytes iteratorWithNumBytes : iterators) {
      if (iteratorWithNumBytes.hasNext()) {
        ret = true;
        break;
      }
    }

    if (!taskAddPairQueue.isEmpty()) {
      return true;
    }

    if (isWatermarkTriggerTime()) {
      return true;
    }

    //LOG.info("{} available {}", stageId, ret);
    return ret;
  }

  @Override
  public Object fetchDataElement() throws IOException, NoSuchElementException {
    if (firstFetch) {
      //fetchDataLazily();
      firstFetch = false;
    }

    //LOG.info("Fetch data for {}", taskId);

    while (!taskAddPairQueue.isEmpty()) {
      final Pair<IteratorWithNumBytes, Integer> pair = taskAddPairQueue.poll();
      //LOG.info("Receive iterator task {} at {} edge {}"
      //  , pair.right(), readersForParentTask.getTaskIndex(), edge.getId());
      final IteratorWithNumBytes iterator = pair.left();
      final int taskIndex = pair.right();

      if (taskIndexIteratorMap.containsKey(taskIndex)) {
        // finish the iterator first!
        final IteratorWithNumBytes finishedIterator = taskIndexIteratorMap.remove(taskIndex);
        iteratorTaskIndexMap.remove(finishedIterator);

        // process remaining data!
        while (finishedIterator.hasNext()) {
          final Object element = iterator.next();
          // data element
          return element;
        }

        LOG.info("Task index {} finished at {}", taskIndex);

        iterators.remove(finishedIterator);
      }

      iteratorTaskIndexMap.put(iterator, taskIndex);
      taskIndexIteratorMap.put(taskIndex, iterator);

      iterators.add(iterator);
    }

    // Emit watermark
    if (isWatermarkTriggerTime()) {
      watermarkTriggered = false;
      final Optional<Long> watermark = rendevousServerClient.requestWatermark(taskId);
      //LOG.info("Request watermark at {}", taskId);

      if (watermark.isPresent() && prevWatermarkTimestamp + Util.WATERMARK_PROGRESS <= watermark.get()) {
        //LOG.info("Receive watermark at {}: {}", taskId, new Instant(watermark.get()));

        prevWatermarkTimestamp = watermark.get();
        return new Watermark(watermark.get());
      }
   }

    int cnt = 0;
    while (cnt < iterators.size()) {
      final IteratorWithNumBytes iterator = iterators.get(iteratorIndex);

      if (iterator.isFinished()) {

        iterators.remove(iteratorIndex);
        final Integer taskIndex = iteratorTaskIndexMap.remove(iterator);
        taskIndexIteratorMap.remove(taskIndex);
        LOG.info("Task index {} finished at {}", taskIndex);
        iteratorIndex = iterators.size() == 0 ? 0 : iteratorIndex % iterators.size();

      } else if (iterator.hasNext()) {
        iteratorIndex = (iteratorIndex + 1) % iterators.size();
        final Object element = iterator.next();

        // data element
        return element;
      } else {
        iteratorIndex = (iteratorIndex + 1) % iterators.size();
        cnt += 1;
      }
    }


    return EmptyElement.getInstance();
  }

  private boolean isWatermarkTriggerTime() {
    return watermarkTriggered;
  }

  @Override
  public Future<Integer> stop(final String taskId) {
    executorGlobalInstances.deregisterWatermarkService(getDataSource());
    return readersForParentTask.stop(taskId);
  }

  @Override
  public void restart() {
    executorGlobalInstances.registerWatermarkService(getDataSource(), () -> {
      final Optional<Long> watermark = rendevousServerClient.requestWatermark(taskId);
      //LOG.info("Request watermark at {}", taskId);

      if (watermark.isPresent() && prevWatermarkTimestamp + Util.WATERMARK_PROGRESS <= watermark.get()) {
        //LOG.info("Receive watermark at {}: {}", taskId, new Instant(watermark.get()));
        prevWatermarkTimestamp = watermark.get();
        taskExecutor.handleIntermediateWatermarkEvent(watermark.get(), this);
      }
    });
    readersForParentTask.restart();
  }

  private void fetchNonBlocking() { // 갯수 동적으로 받아야함. handler 같은거 등록하기

    readersForParentTask.readAsync(taskId, pair -> {
      //LOG.info("Task input context added {}", taskId, pair.left());
      taskAddPairQueue.add(pair);
    });
  }


  private void fetchAsync() { // 갯수 동적으로 받아야함. handler 같은거 등록하기


    readersForParentTask.readAsync(taskId, pair -> {

      LOG.info("Receive iterator task {} at {} edge {}"
        , pair.right(), readersForParentTask.getTaskIndex(), edge.getId());
      final IteratorWithNumBytes iterator = pair.left();
      final int taskIndex = pair.right();

      queueInsertionThreads.submit(() -> {
        // Consume this iterator to the end.
        while (iterator.hasNext()) {
          // blocked on the iterator.
          final Object element = iterator.next();
            // data element
            elementQueue.offer(element);
        }

        // This iterator is finished.
        LOG.info("Task index {} finished at {}", taskIndex);
      });
    });
  }

  private void fetchBlocking() {
    // 갯수 동적으로 받아야함. handler 같은거 등록하기
    final List<IteratorWithNumBytes> inputs = readersForParentTask.readBlocking();
    numOfIterators = inputs.size();

    for (final IteratorWithNumBytes iterator : inputs) {
      // A thread for each iterator
      queueInsertionThreads.submit(() -> {
        // Consume this iterator to the end.
        while (iterator.hasNext()) { // blocked on the iterator.
          final Object element = iterator.next();
          // data element
          elementQueue.offer(element);
        }

        // This iterator is finished.
        countBytesSynchronized(iterator);
        elementQueue.offer(Finishmark.getInstance());
      });
    }
  }


  private void fetchDataLazily() {
    final List<CompletableFuture<IteratorWithNumBytes>> futures = readersForParentTask.read();
    numOfIterators = futures.size();

    futures.forEach(compFuture -> compFuture.whenComplete((iterator, exception) -> {
      // A thread for each iterator
      queueInsertionThreads.submit(() -> {
        if (exception == null) {
          // Consume this iterator to the end.
          while (iterator.hasNext()) { // blocked on the iterator.
            final Object element = iterator.next();
            // data element
            elementQueue.offer(element);
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

  public final long getSerializedBytes() {
    return serBytes;
  }

  public final long getEncodedBytes() {
    return encodedBytes;
  }

  private synchronized void countBytesSynchronized(final IteratorWithNumBytes iterator) {
    try {
      serBytes += iterator.getNumSerializedBytes();
    } catch (final IteratorWithNumBytes.NumBytesNotSupportedException e) {
      serBytes = -1;
    } catch (final IllegalStateException e) {
      LOG.error("Failed to get the number of bytes of serialized data - the data is not ready yet ", e);
    }
    try {
      encodedBytes += iterator.getNumEncodedBytes();
    } catch (final IteratorWithNumBytes.NumBytesNotSupportedException e) {
      encodedBytes = -1;
    } catch (final IllegalStateException e) {
      LOG.error("Failed to get the number of bytes of encoded data - the data is not ready yet ", e);
    }
  }



  @Override
  public void close() throws Exception {
    queueInsertionThreads.shutdown();
  }

  @Override
  public String toString() {
    return "dataFetcher" + taskId;
  }
}
