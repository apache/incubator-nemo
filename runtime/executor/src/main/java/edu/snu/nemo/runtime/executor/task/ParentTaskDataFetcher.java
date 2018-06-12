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

import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.exception.BlockFetchException;
import edu.snu.nemo.runtime.executor.data.DataUtil;
import edu.snu.nemo.runtime.executor.datatransfer.InputReader;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

class ParentTaskDataFetcher extends DataFetcher {
  private final List<InputReader> readersForParentTasks;

  // Non-finals (lazy fetching)
  private LinkedBlockingQueue<DataUtil.IteratorWithNumBytes> dataQueue;
  private int expectedNumOfIterators;
  private boolean hasFetchStarted;

  ParentTaskDataFetcher(final List<VertexHarness> children,
                        final List<InputReader> readersForParentTasks) {
    super(children);
    this.readersForParentTasks = readersForParentTasks;
    this.hasFetchStarted = false;
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
    if (!hasFetchStarted) {
      fetchInBackground();
      hasFetchStarted = true;
    }



    // Process data from other stages.
    for (int currPartition = 0; currPartition < numPartitionsFromOtherStages; currPartition++) {
      final DataUtil.IteratorWithNumBytes iterator = dataQueue.take();
      try {
        return iterator.next();
      } catch (final NoSuchElementException e) {
        return null;
      }

      /*
      // Collect metrics on block size if possible.
      try {
        serBlockSize += iterator.getNumSerializedBytes();
      } catch (final DataUtil.IteratorWithNumBytes.NumBytesNotSupportedException e) {
        serBlockSize = -1;
      } catch (final IllegalStateException e) {
        LOG.error("Failed to get the number of bytes of serialized data - the data is not ready yet ", e);
      }
      try {
        encodedBlockSize += iterator.getNumEncodedBytes();
      } catch (final DataUtil.IteratorWithNumBytes.NumBytesNotSupportedException e) {
        encodedBlockSize = -1;
      } catch (final IllegalStateException e) {
        LOG.error("Failed to get the number of bytes of encoded data - the data is not ready yet ", e);
      }
      */
    }
    /*
    inputReadEndTime = System.currentTimeMillis();
    metric.put("InputReadTime(ms)", inputReadEndTime - inputReadStartTime);
    */
  }
}
