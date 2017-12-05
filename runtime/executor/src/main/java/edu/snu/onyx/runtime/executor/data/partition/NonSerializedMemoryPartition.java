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
 * This class represents a partition which is stored in local memory and not serialized.
 */
@ThreadSafe
public final class NonSerializedMemoryPartition implements Partition {

  private final List<NonSerializedBlock> nonSerializedBlocks;
  private final Coder coder;
  private volatile boolean committed;

  public NonSerializedMemoryPartition(final Coder coder) {
    this.nonSerializedBlocks = new ArrayList<>();
    this.coder = coder;
    this.committed = false;
  }

  /**
   * Stores {@link NonSerializedBlock}s to this partition.
   * Invariant: This should not be invoked after this partition is committed.
   *
   * @param blocksToStore the {@link NonSerializedBlock}s to store.
   * @throws IOException if fail to store.
   */
  @Override
  public synchronized Optional<List<Long>> putBlocks(final Iterable<NonSerializedBlock> blocksToStore)
      throws IOException {
    if (!committed) {
      blocksToStore.forEach(nonSerializedBlocks::add);
    } else {
      throw new IOException("Cannot append block to the committed partition");
    }

    return Optional.empty();
  }

  /**
   * Stores {@link SerializedBlock}s to this partition.
   * Because all data in this partition is stored in a non-serialized form,
   * the data in these blocks have to be deserialized.
   * Invariant: This should not be invoked after this partition is committed.
   *
   * @param blocksToStore the {@link SerializedBlock}s to store.
   * @throws IOException if fail to store.
   */
  @Override
  public synchronized List<Long> putSerializedBlocks(final Iterable<SerializedBlock> blocksToStore)
      throws IOException {
    if (!committed) {
      final Iterable<NonSerializedBlock> convertedBlocks = DataUtil.convertToNonSerBlocks(coder, blocksToStore);
      final List<Long> dataSizePerBlock = new ArrayList<>();
      blocksToStore.forEach(serializedBlock -> dataSizePerBlock.add((long) serializedBlock.getData().length));
      putBlocks(convertedBlocks);

      return dataSizePerBlock;
    } else {
      throw new IOException("Cannot append nonSerializedBlocks to the committed partition");
    }
  }

  /**
   * Retrieves the {@link NonSerializedBlock}s in a specific hash range from this partition.
   * Invariant: This should not be invoked before this partition is committed.
   *
   * @param hashRange the hash range to retrieve.
   * @return an iterable of {@link NonSerializedBlock}s.
   * @throws IOException if failed to retrieve.
   */
  @Override
  public Iterable<NonSerializedBlock> getBlocks(final HashRange hashRange) throws IOException {
    if (committed) {
      // Retrieves data in the hash range from the target partition
      final List<NonSerializedBlock> retrievedBlocks = new ArrayList<>();
      nonSerializedBlocks.forEach(block -> {
        final int key = block.getKey();
        if (hashRange.includes(key)) {
          retrievedBlocks.add(new NonSerializedBlock(key, block.getData()));
        }
      });

      return retrievedBlocks;
    } else {
      throw new IOException("Cannot retrieve elements before a partition is committed");
    }
  }

  /**
   * Retrieves the {@link SerializedBlock}s in a specific hash range.
   * Because the data is stored in a non-serialized form, it have to be serialized.
   * Invariant: This should not be invoked before this partition is committed.
   *
   * @param hashRange the hash range to retrieve.
   * @return an iterable of {@link SerializedBlock}s.
   * @throws IOException if failed to retrieve.
   */
  @Override
  public Iterable<SerializedBlock> getSerializedBlocks(final HashRange hashRange) throws IOException {
    return DataUtil.convertToSerBlocks(coder, getBlocks(hashRange));
  }

  /**
   * Commits this partition to prevent further write.
   */
  @Override
  public synchronized void commit() {
    committed = true;
  }
}
