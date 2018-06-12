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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

class ParentTaskDataFetcher extends DataFetcher {
  private final List<InputReader> readersForParentTasks;(
  private final LinkedBlockingQueue<DataUtil.IteratorWithNumBytes> dataQueue;

  ParentTaskDataFetcher(final List<VertexHarness> children,
                        final List<InputReader> readersForParentTasks) {
    super(children);
    this.readersForParentTasks = readersForParentTasks;

    readersForParentTasks.stream().map(InputReader::read);

    final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = inputReader.read();
    numPartitionsFromOtherStages += futures.size();

    // Add consumers which will push iterator when the futures are complete.
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
  Object fetchDataElement() throws Exception {
    // Process data from other stages.
    for (int currPartition = 0; currPartition < numPartitionsFromOtherStages; currPartition++) {
      Pair<String, DataUtil.IteratorWithNumBytes> idToIteratorPair = dataQueue.take();
      final String iteratorId = idToIteratorPair.left();
      final DataUtil.IteratorWithNumBytes iterator = idToIteratorPair.right();
      List<VertexHarness> vertexHarnesses = iteratorIdToDataHandlersMap.get(iteratorId);
      iterator.forEachRemaining(element -> {
        for (final VertexHarness vertexHarness : vertexHarnesses) {
          processElementRecursively(vertexHarness, element);
        }
      });

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
    }
    inputReadEndTime = System.currentTimeMillis();
    metric.put("InputReadTime(ms)", inputReadEndTime - inputReadStartTime);
  }
}
