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
package edu.snu.vortex.runtime.executor.data.metadata;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This class represents a metadata for a local file partition.
 * It resides in local only, and does not synchronize with master.
 */
@ThreadSafe
public final class LocalFileMetadata extends FileMetadata {

  // When a writer reserves a file region for a block to write, the metadata of the block is stored in this queue.
  // When a block in this queue is committed, the committed blocks are polled and go into the committed iterable.
  private final Queue<BlockMetadata> reserveBlockMetadataQue;
  // TODO #463: Support incremental read. Change this iterable to "ClosableBlockingIterable".
  private final List<BlockMetadata> commitBlockMetadataIterable; // The queue of committed block metadata.
  private volatile long position; // How many bytes are (at least, logically) written in the file.
  private volatile int blockCount;
  private volatile boolean committed;

  public LocalFileMetadata(final boolean commitPerBlock) {
    super(commitPerBlock);
    this.reserveBlockMetadataQue = new ArrayDeque<>();
    this.commitBlockMetadataIterable = new CopyOnWriteArrayList<>();
    this.blockCount = 0;
    this.position = 0;
    this.committed = false;
  }

  /**
   * Reserves the region for a block and get the metadata for the block.
   * @see FileMetadata#reserveBlock(int, int, long).
   */
  @Override
  public synchronized BlockMetadata reserveBlock(final int hashValue,
                                                 final int blockSize,
                                                 final long elementsTotal) throws IOException {
    if (committed) {
      throw new IOException("Cannot write a new block to a closed partition.");
    }

    final BlockMetadata blockMetadata =
        new BlockMetadata(blockCount, hashValue, blockSize, position, elementsTotal);
    reserveBlockMetadataQue.add(blockMetadata);
    blockCount++;
    position += blockSize;
    return blockMetadata;
  }

  /**
   * Notifies that some blocks are written.
   * @see FileMetadata#commitBlocks(Iterable).
   */
  @Override
  public synchronized void commitBlocks(final Iterable<BlockMetadata> blockMetadataToCommit) {
    blockMetadataToCommit.forEach(BlockMetadata::setCommitted);

    while (!reserveBlockMetadataQue.isEmpty() && reserveBlockMetadataQue.peek().isCommitted()) {
      // If the metadata in the top of the reserved queue is committed, move it to the committed metadata iterable.
      commitBlockMetadataIterable.add(reserveBlockMetadataQue.poll());
    }
  }

  /**
   * Gets a iterable containing the block metadata of corresponding partition.
   * @see FileMetadata#getBlockMetadataIterable().
   */
  @Override
  public Iterable<BlockMetadata> getBlockMetadataIterable() {
    return commitBlockMetadataIterable;
  }

  /**
   * @see FileMetadata#deleteMetadata().
   */
  @Override
  public void deleteMetadata() {
    // Do nothing because this metadata is only in the local memory.
  }

  /**
   * Notifies that all writes are finished for the partition corresponding to this metadata.
   * Subscribers waiting for the data of the target partition are notified when the partition is committed.
   * Also, further subscription about a committed partition will not blocked but get the data in it and finished.
   */
  @Override
  public synchronized void commitPartition() {
    // TODO #463: Support incremental write. Close the "ClosableBlockingIterable".
  }
}
