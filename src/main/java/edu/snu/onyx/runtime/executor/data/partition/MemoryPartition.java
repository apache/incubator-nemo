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

import edu.snu.onyx.runtime.executor.data.Block;
import edu.snu.onyx.runtime.executor.data.HashRange;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * This class represents a partition which is stored in local memory and not serialized.
 */
@ThreadSafe
public final class MemoryPartition implements Partition {

  private final List<Block> blocks;
  private volatile boolean committed;

  public MemoryPartition() {
    blocks = new ArrayList<>();
    committed = false;
  }

  /**
   * Writes {@link Block}s to this partition.
   * Invariant: This should not be invoked after this partition is committed.
   *
   * @param blocksToWrite the {@link Block}s to write.
   * @throws IOException if fail to write.
   */
  @Override
  public synchronized Optional<List<Long>> putBlocks(final Iterable<Block> blocksToWrite) throws IOException {
    if (!committed) {
      blocksToWrite.forEach(blocks::add);
    } else {
      throw new IOException("Cannot append blocks to the committed partition");
    }

    return Optional.empty();
  }

  /**
   * Retrieves the blocks in a specific hash range and deserializes it from this partition.
   * Invariant: This should not be invoked before this partition is committed.
   *
   * @param hashRange the hash range to retrieve.
   * @return an iterable of deserialized blocks.
   * @throws IOException if failed to deserialize.
   */
  @Override
  public Iterable<Block> getBlocks(final HashRange hashRange) throws IOException {
    if (committed) {
      // Retrieves data in the hash range from the target partition
      final List<Block> retrievedBlocks = new ArrayList<>();
      blocks.forEach(block -> {
        final int key = block.getKey();
        if (hashRange.includes(key)) {
          retrievedBlocks.add(new Block(key, block.getElements()));
        }
      });

      return retrievedBlocks;
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
