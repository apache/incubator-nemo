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
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.punctuation.Finishmark;
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.apache.nemo.runtime.executor.datatransfer.InputReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;

/**
 * Fetches data from parent tasks.
 */
@NotThreadSafe
class MultiThreadParentTaskDataFetcher extends DataFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(MultiThreadParentTaskDataFetcher.class);
  private static final int ELEMENT_QUEUE_CAPACITY = 50;

  private final InputReader readersForParentTask;

  // Non-finals (lazy fetching)
  private boolean firstFetch = true;
  private ExecutorService queueInsertionThreads;

  private final ArrayBlockingQueue elementQueue;

  private long serBytes = 0;
  private long encodedBytes = 0;

  private int numOfIterators;
  private int numOfFinishMarks = 0;

  MultiThreadParentTaskDataFetcher(final IRVertex dataSource,
                                   final InputReader readerForParentTask,
                                   final OutputCollector outputCollector) {
    super(dataSource, outputCollector);
    this.readersForParentTask = readerForParentTask;
    this.firstFetch = true;
    this.elementQueue = new ArrayBlockingQueue(ELEMENT_QUEUE_CAPACITY);
  }

  @Override
  Object fetchDataElement() throws IOException {
    if (firstFetch) {
      fetchDataLazily();
      firstFetch = false;
    }

    LOG.info("finish {} iter {}", numOfFinishMarks, numOfIterators);

    try {
      while (true) {
        LOG.info("queue take");
        final Object element = elementQueue.take();
        LOG.info("Got element {}", element);
        final boolean isFinishMark = element instanceof Finishmark;
        if (isFinishMark) {
          LOG.info("isfinishmark: finish {} iter {}", numOfFinishMarks, numOfIterators);
          numOfFinishMarks++;
          if (numOfFinishMarks == numOfIterators) {
            // LOG.info("return finishmark", numOfFinishMarks, numOfIterators);
            return Finishmark.getInstance();
          }
          // LOG.info("else try again", numOfFinishMarks, numOfIterators);
          // else try again.
        } else {
          LOG.info("return element: finish {} iter {}", numOfFinishMarks, numOfIterators);
          return element;
        }
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  private void fetchDataLazily() {
    final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = readersForParentTask.read();
    numOfIterators = futures.size();

    queueInsertionThreads = Executors.newFixedThreadPool(numOfIterators);
    futures.forEach(compFuture -> compFuture.whenComplete((iterator, exception) -> {
      // A thread for each iterator
      LOG.info("Monitor {} with {}", iterator.toString(), numOfIterators);

      LOG.info("Submitting {}", iterator.toString());
      LOG.info("queue threads is {} and the num is {}", queueInsertionThreads.toString(), numOfIterators);

      queueInsertionThreads.submit(() -> {
        LOG.info("Submitted {}", iterator.toString());

        if (exception == null) {
          try {
            // Consume this iterator to the end.
            while (iterator.hasNext()) { // blocked on the iterator.
              final Object element = iterator.next();
              LOG.info("Putting {}", element);
              elementQueue.put(element); // blocked on the queue.
            }

            // This iterator is finished.
            LOG.info("Done {}", iterator.toString());
            countBytesSynchronized(iterator);
            elementQueue.put(Finishmark.getInstance());
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e); // this should not happen.
          }
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
}
