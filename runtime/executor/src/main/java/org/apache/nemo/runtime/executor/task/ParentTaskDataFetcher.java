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

import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.apache.nemo.runtime.executor.datatransfer.InputReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Fetches data from parent tasks.
 */
@NotThreadSafe
class ParentTaskDataFetcher extends DataFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(ParentTaskDataFetcher.class);

  private final InputReader readersForParentTask;
  private final LinkedBlockingQueue iteratorQueue;

  // Non-finals (lazy fetching)
  private boolean firstFetch;
  private int expectedNumOfIterators;
  private DataUtil.IteratorWithNumBytes currentIterator;
  private int currentIteratorIndex;
  private long serBytes = 0;
  private long encodedBytes = 0;

  ParentTaskDataFetcher(final IRVertex dataSource, final InputReader readerForParentTask, final VertexHarness child) {
    super(dataSource, child);
    this.readersForParentTask = readerForParentTask;
    this.firstFetch = true;
    this.currentIteratorIndex = 0;
    this.iteratorQueue = new LinkedBlockingQueue<>();
  }

  @Override
  Object fetchDataElement() throws IOException {
    try {
      if (firstFetch) {
        fetchDataLazily();
        advanceIterator();
        firstFetch = false;
      }

      while (true) {
        // This iterator has the element
        if (this.currentIterator.hasNext()) {
          return this.currentIterator.next();
        }

        // This iterator does not have the element
        if (currentIteratorIndex < expectedNumOfIterators) {
          // Next iterator has the element
          countBytes(currentIterator);
          advanceIterator();
          continue;
        } else {
          // We've consumed all the iterators
          break;
        }
      }
    } catch (final Throwable e) {
      // Any failure is caught and thrown as an IOException, so that the task is retried.
      // In particular, we catch unchecked exceptions like RuntimeException thrown by DataUtil.IteratorWithNumBytes
      // when remote data fetching fails for whatever reason.
      // Note that we rely on unchecked exceptions because the Iterator interface does not provide the standard
      // "throw Exception" that the TaskExecutor thread can catch and handle.
      throw new IOException(e);
    }

    return Finishmark.getInstance();
  }

  private void advanceIterator() throws IOException {
    // Take from iteratorQueue
    final Object iteratorOrThrowable;
    try {
      iteratorOrThrowable = iteratorQueue.take(); // blocking call
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }

    // Handle iteratorOrThrowable
    if (iteratorOrThrowable instanceof Throwable) {
      throw new IOException((Throwable) iteratorOrThrowable);
    } else {
      // This iterator is valid. Do advance.
      this.currentIterator = (DataUtil.IteratorWithNumBytes) iteratorOrThrowable;
      this.currentIteratorIndex++;
    }
  }

  private void fetchDataLazily() throws IOException {
    final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = readersForParentTask.read();
    this.expectedNumOfIterators = futures.size();

    futures.forEach(compFuture -> compFuture.whenComplete((iterator, exception) -> {
      try {
        if (exception != null) {
          iteratorQueue.put(exception);
        } else {
          iteratorQueue.put(iterator);
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e); // this should not happen
      }
    }));
  }

  final long getSerializedBytes() {
    return serBytes;
  }

  final long getEncodedBytes() {
    return encodedBytes;
  }

  private void countBytes(final DataUtil.IteratorWithNumBytes iterator) {
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

  }
}
