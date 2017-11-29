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
package edu.snu.onyx.runtime.executor.data.metadata;

import java.io.IOException;

/**
 * This class represents a metadata for a (local / remote) file partition.
 * The writer and reader determine the status of a file partition
 * (such as accessibility, how many bytes are written, etc.) by using this metadata.
 */
public abstract class FileMetadata {

  private final boolean blockCommitPerWrite; // Whether need to commit block per every block write or not.

  protected FileMetadata(final boolean blockCommitPerWrite) {
    this.blockCommitPerWrite = blockCommitPerWrite;
  }

  /**
   * Reserves the region for a block and get the metadata for the block.
   * When a writer reserves the region (or space) of a file for a data block,
   * other writers will write their data after the region.
   * Also, the readers will judge a data block available after the block is committed.
   *
   * @param hashValue     the hash range of the block.
   * @param blockSize     the size of the block.
   * @param elementsTotal the number of elements in the block.
   * @return the {@link BlockMetadata} having all given information, the block offset, and the index.
   * @throws IOException if fail to append the block metadata.
   */
  public abstract BlockMetadata reserveBlock(final int hashValue,
                                             final int blockSize,
                                             final long elementsTotal) throws IOException;

  /**
   * Notifies that some blocks are written.
   *
   * @param blockMetadataToCommit the block metadata of the blocks to commit.
   */
  public abstract void commitBlocks(final Iterable<BlockMetadata> blockMetadataToCommit);

  /**
   * Gets a iterable containing the block metadata of corresponding partition.
   * It returns a "blocking iterable" to which metadata for blocks that become available will be published.
   *
   * @return the "blocking iterable" containing the block metadata.
   * @throws IOException if fail to get the iterable.
   */
  public abstract Iterable<BlockMetadata> getBlockMetadataIterable() throws IOException;

  /**
   * Deletes the metadata.
   *
   * @throws IOException if fail to delete.
   */
  public abstract void deleteMetadata() throws IOException;

  /**
   * @return whether commit every block write or not.
   */
  public final boolean isBlockCommitPerWrite() {
    return blockCommitPerWrite;
  }

  /**
   * Notifies that all writes are finished for the partition corresponding to this metadata.
   * Subscribers waiting for the data of the target partition are notified when the partition is committed.
   * Also, further subscription about a committed partition will not blocked but get the data in it and finished.
   */
  public abstract void commitPartition();
}
