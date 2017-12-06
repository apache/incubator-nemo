/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.onyx.runtime.executor.data.partition;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.runtime.executor.data.DataUtil;
import edu.snu.onyx.runtime.common.data.HashRange;
import edu.snu.onyx.runtime.executor.data.NonSerializedBlock;
import edu.snu.onyx.runtime.executor.data.SerializedBlock;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * This class represents a partition which is serialized and stored in local memory.
 */
@ThreadSafe
public final class SerializedMemoryPartition implements Partition {

  private final List<SerializedBlock> serializedBlocks;
  private final Coder coder;
  private volatile boolean committed;

  public SerializedMemoryPartition(final Coder coder) {
    this.coder = coder;
    serializedBlocks = new ArrayList<>();
    committed = false;
  }

  /**
   * Serialized and stores {@link NonSerializedBlock}s to this partition.
   * Invariant: This should not be invoked after this partition is committed.
   *
   * @param blocksToStore the {@link NonSerializedBlock}s to store.
   * @return the size of the data per block.
   * @throws IOException if fail to store.
   */
  @Override
  public synchronized Optional<List<Long>> putBlocks(final Iterable<NonSerializedBlock> blocksToStore)
      throws IOException {
    if (!committed) {
      final Iterable<SerializedBlock> convertedBlocks = DataUtil.convertToSerBlocks(coder, blocksToStore);

      return Optional.of(putSerializedBlocks(convertedBlocks));
    } else {
      throw new IOException("Cannot append blocks to the committed partition");
    }
  }

  /**
   * Stores {@link SerializedBlock}s to this partition.
   * Invariant: This should not be invoked after this partition is committed.
   *
   * @param blocksToWrite the {@link SerializedBlock}s to store.
   * @throws IOException if fail to store.
   */
  @Override
  public synchronized List<Long> putSerializedBlocks(final Iterable<SerializedBlock> blocksToWrite)
      throws IOException {
    if (!committed) {
      final List<Long> blockSizeList = new ArrayList<>();
      blocksToWrite.forEach(serializedBlock -> {
        blockSizeList.add((long) serializedBlock.getLength());
        serializedBlocks.add(serializedBlock);
      });

      return blockSizeList;
    } else {
      throw new IOException("Cannot append blocks to the committed partition");
    }
  }

  /**
   * Retrieves the {@link NonSerializedBlock}s in a specific hash range from this partition.
   * Because the data is stored in a serialized form, it have to be deserialized.
   * Invariant: This should not be invoked before this partition is committed.
   *
   * @param hashRange the hash range to retrieve.
   * @return an iterable of {@link NonSerializedBlock}s.
   * @throws IOException if failed to retrieve.
   */
  @Override
  public Iterable<NonSerializedBlock> getBlocks(final HashRange hashRange) throws IOException {
    return DataUtil.convertToNonSerBlocks(coder, getSerializedBlocks(hashRange));
  }

  /**
   * Retrieves the {@link SerializedBlock}s in a specific hash range.
   * Invariant: This should not be invoked before this partition is committed.
   *
   * @param hashRange the hash range to retrieve.
   * @return an iterable of {@link SerializedBlock}s.
   * @throws IOException if failed to retrieve.
   */
  @Override
  public Iterable<SerializedBlock> getSerializedBlocks(final HashRange hashRange) throws IOException {
    if (committed) {
      final List<SerializedBlock> blocksInRange = new ArrayList<>();
      serializedBlocks.forEach(serializedBlock -> {
        final int hashVal = serializedBlock.getKey();
        if (hashRange.includes(hashVal)) {
          // The hash value of this block is in the range.
          blocksInRange.add(serializedBlock);
        }
      });

      return blocksInRange;
    } else {
      throw new IOException("Cannot retrieve elements before a partition is committed");
    }
  }

  /**
   * Commits this partition to prevent further write.
   */
  @Override
  public synchronized void commit() {
    committed = true;
  }
}
