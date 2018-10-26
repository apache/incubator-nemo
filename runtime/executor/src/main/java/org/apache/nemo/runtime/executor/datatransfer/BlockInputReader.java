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
package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.common.HashRange;
import org.apache.nemo.common.KeyRange;
import org.apache.nemo.common.exception.BlockFetchException;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.executor.data.BlockManagerWorker;
import org.apache.nemo.runtime.executor.data.DataUtil;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Represents the input data transfer to a task.
 */
public final class BlockInputReader extends InputReader {
  private final BlockManagerWorker blockManagerWorker;

  public BlockInputReader(final int dstTaskIndex,
                          final IRVertex srcVertex,
                          final RuntimeEdge runtimeEdge,
                          final BlockManagerWorker blockManagerWorke) {
    super(dstTaskIndex, srcVertex, runtimeEdge);
    this.blockManagerWorker = blockManagerWorke;
  }

  @Override
  CompletableFuture<DataUtil.IteratorWithNumBytes> readOneToOne() {
    final String blockIdWildcard = generateWildCardBlockId(getDstTaskIndex());
    final Optional<DataStoreProperty.Value> dataStoreProperty
        = getRuntimeEdge().getPropertyValue(DataStoreProperty.class);
    return blockManagerWorker.readBlock(blockIdWildcard, getId(), dataStoreProperty.get(), HashRange.all());
  }

  @Override
  List<CompletableFuture<DataUtil.IteratorWithNumBytes>> readBroadcast() {
    final int numSrcTasks = this.getSourceParallelism();
    final Optional<DataStoreProperty.Value> dataStoreProperty
        = getRuntimeEdge().getPropertyValue(DataStoreProperty.class);

    final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final String blockIdWildcard = generateWildCardBlockId(srcTaskIdx);
      futures.add(blockManagerWorker.readBlock(blockIdWildcard, getId(), dataStoreProperty.get(), HashRange.all()));
    }

    return futures;
  }

  /**
   * Read data in the assigned range of hash value.
   *
   * @return the list of the completable future of the data.
   */
  @Override
  List<CompletableFuture<DataUtil.IteratorWithNumBytes>> readDataInRange() {
    assert (getRuntimeEdge() instanceof StageEdge);
    final Optional<DataStoreProperty.Value> dataStoreProperty
        = getRuntimeEdge().getPropertyValue(DataStoreProperty.class);
    ((StageEdge) getRuntimeEdge()).getTaskIdxToKeyRange().get(getDstTaskIndex());
    final KeyRange hashRangeToRead = ((StageEdge) getRuntimeEdge()).getTaskIdxToKeyRange().get(getDstTaskIndex());
    if (hashRangeToRead == null) {
      throw new BlockFetchException(
          new Throwable("The hash range to read is not assigned to " + getDstTaskIndex() + "'th task"));
    }

    final int numSrcTasks = this.getSourceParallelism();
    final List<CompletableFuture<DataUtil.IteratorWithNumBytes>> futures = new ArrayList<>();
    for (int srcTaskIdx = 0; srcTaskIdx < numSrcTasks; srcTaskIdx++) {
      final String blockIdWildcard = generateWildCardBlockId(srcTaskIdx);
      futures.add(
          blockManagerWorker.readBlock(blockIdWildcard, getId(), dataStoreProperty.get(), hashRangeToRead));
    }

    return futures;
  }
}
