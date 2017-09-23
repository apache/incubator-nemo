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
package edu.snu.vortex.runtime.executor.data.partition;

import edu.snu.vortex.runtime.executor.data.Block;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class represents a partition which is stored in local memory and not serialized.
 */
@ThreadSafe
public final class MemoryPartition {

  private final List<Block> blocks;
  private volatile AtomicBoolean committed;

  public MemoryPartition() {
    blocks = Collections.synchronizedList(new ArrayList<>());
    committed = new AtomicBoolean(false);
  }

  /**
   * Appends all data in the block to this partition.
   *
   * @param blocksToAppend the blocks to append.
   * @throws IOException if this partition is committed already.
   */
  public void appendBlocks(final Iterable<Block> blocksToAppend) throws IOException {
    if (!committed.get()) {
      // TODO #463: Support incremental write.
      blocksToAppend.forEach(blocks::add);
    } else {
      throw new IOException("Cannot append blocks to the committed partition");
    }
  }

  /**
   * @return the list of the blocks in this partition.
   */
  public List<Block> getBlocks() {
    return blocks;
  }

  /**
   * Commits this partition to prevent further write.
   * If someone "subscribing" the data in this partition, it will be finished.
   */
  public void commit() {
    committed.set(true);
  }
}
