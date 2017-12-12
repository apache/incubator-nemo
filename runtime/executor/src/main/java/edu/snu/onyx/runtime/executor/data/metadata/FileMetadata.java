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
 * This class represents a metadata for a {@link edu.snu.onyx.runtime.executor.data.block.Block}.
 * The writer and reader determine the status of a file block
 * (such as accessibility, how many bytes are written, etc.) by using this metadata.
 */
public abstract class FileMetadata {

  private final boolean partitionCommitPerWrite; // Whether need to commit partition per every block write or not.

  protected FileMetadata(final boolean partitionCommitPerWrite) {
    this.partitionCommitPerWrite = partitionCommitPerWrite;
  }

  /**
   * Reserves the region for a partition and get the metadata for the partition.
   * When a writer reserves the region (or space) of a file for a data partition,
   * other writers will write their data after the region.
   * Also, the readers will judge a data partition available after the partition is committed.
   *
   * @param hashValue     the hash range of the block.
   * @param partitionSize the size of the block.
   * @param elementsTotal the number of elements in the partition.
   * @return the {@link PartitionMetadata} having all given information, the partition offset, and the index.
   * @throws IOException if fail to append the partition metadata.
   */
  public abstract PartitionMetadata reservePartition(final int hashValue,
                                                     final int partitionSize,
                                                     final long elementsTotal) throws IOException;

  /**
   * Notifies that some partitions are written.
   *
   * @param partitionMetadataToCommit the metadata of the partitions to commit.
   */
  public abstract void commitPartitions(final Iterable<PartitionMetadata> partitionMetadataToCommit);

  /**
   * Gets a iterable containing the partition metadata of corresponding block.
   *
   * @return the iterable containing the partition metadata.
   * @throws IOException if fail to get the iterable.
   */
  public abstract Iterable<PartitionMetadata> getPartitionMetadataIterable() throws IOException;

  /**
   * Deletes the metadata.
   *
   * @throws IOException if fail to delete.
   */
  public abstract void deleteMetadata() throws IOException;

  /**
   * @return whether commit every partition write or not.
   */
  public final boolean isPartitionCommitPerWrite() {
    return partitionCommitPerWrite;
  }

  /**
   * Notifies that all writes are finished for the block corresponding to this metadata.
   */
  public abstract void commitBlock();
}
