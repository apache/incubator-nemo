/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.runtime.executor.task;

import edu.snu.nemo.common.exception.BlockFetchException;
import edu.snu.nemo.runtime.executor.data.DataUtil;
import edu.snu.nemo.runtime.executor.datatransfer.InputReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * Fetches data from parent tasks.
 */
class ParentTaskDataFetcher extends DataFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(ParentTaskDataFetcher.class.getName());

  private final List<InputReader> readersForParentTasks;
  private final LinkedBlockingQueue<DataUtil.IteratorWithNumBytes> dataQueue;

  // Non-finals (lazy fetching)
  private boolean hasFetchStarted;
  private int expectedNumOfIterators;
  private DataUtil.IteratorWithNumBytes currentIterator;
  private int currentIteratorIndex;

  ParentTaskDataFetcher(final List<InputReader> readersForParentTasks,
                        final List<VertexHarness> children,
                        final Map<String, Object> metricMap,
                        final boolean isForSideInput) {
    super(children, metricMap, isForSideInput);
    this.readersForParentTasks = readersForParentTasks;
    this.hasFetchStarted = false;
    this.dataQueue = new LinkedBlockingQueue<>();
  }

  private void handleMetric(final DataUtil.IteratorWithNumBytes iterator) {
    long serBytes = 0;
    long encodedBytes = 0;
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
    if (serBytes != encodedBytes) {
      getMetricMap().put("ReadBytes(raw)", serBytes);
    }
    getMetricMap().put("ReadBytes", encodedBytes);
  }

  private void fetchInBackground() {
    final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = readersForParentTasks.stream()
        .map(InputReader::read)
        .flatMap(List::stream)
        .collect(Collectors.toList());
    this.expectedNumOfIterators = futures.size();

    futures.forEach(compFuture -> compFuture.whenComplete((iterator, exception) -> {
      if (exception != null) {
        throw new BlockFetchException(exception);
      }

      handleMetric(iterator);

      try {
        dataQueue.put(iterator);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new BlockFetchException(e);
      }
    }));
  }

  @Override
  Object fetchDataElement() throws IOException {
    try {
      if (!hasFetchStarted) {
        fetchInBackground();
        hasFetchStarted = true;
        this.currentIterator = dataQueue.take();
        this.currentIteratorIndex = 1;
      }

      if (this.currentIterator.hasNext()) {
        return this.currentIterator.next();
      } else {
        // This iterator is done, proceed to the next iterator
        if (currentIteratorIndex == expectedNumOfIterators) {
          // No more iterator left
          return null;
        } else {
          // Try the next iterator
          this.currentIteratorIndex += 1;
          this.currentIterator = dataQueue.take();
          return fetchDataElement();
        }
      }
    } catch (InterruptedException exception) {
      throw new IOException(exception);
    }
  }
}
