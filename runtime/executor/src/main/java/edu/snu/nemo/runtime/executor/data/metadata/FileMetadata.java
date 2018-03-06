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
package edu.snu.nemo.runtime.executor.data.metadata;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class represents a metadata for a {@link edu.snu.nemo.runtime.executor.data.block.Block}.
 * The writer and reader determine the status of a file block
 * (such as accessibility, how many bytes are written, etc.) by using this metadata.
 * @param <K> the key type of its partitions.
 */
public abstract class FileMetadata<K extends Serializable> {

  private final List<PartitionMetadata<K>> partitionMetadataList; // The list of partition metadata.
  private final AtomicBoolean committed;
  private volatile long writtenBytesCursor; // Indicates how many bytes are (at least, logically) written in the file.

  /**
   * Construct a new file metadata.
   */
  public FileMetadata() {
    this.partitionMetadataList = new ArrayList<>();
    this.writtenBytesCursor = 0;
    this.committed = new AtomicBoolean(false);
  }

  /**
   * Construct a file metadata with existing partition metadata.
   * @param partitionMetadataList the partition metadata list.
   */
  public FileMetadata(final List<PartitionMetadata<K>> partitionMetadataList) {
    this.partitionMetadataList = partitionMetadataList;
    this.writtenBytesCursor = 0;
    this.committed = new AtomicBoolean(true);
  }

  /**
   * Writes the metadata for a partition.
   *
   * @param key     the key of the partition.
   * @param partitionSize the size of the partition.
   * @param elementsTotal the number of elements in the partition.
   * @throws IOException if fail to append the partition metadata.
   */
  public final synchronized void writePartitionMetadata(final K key,
                                                        final int partitionSize,
                                                        final long elementsTotal) throws IOException {
    if (committed.get()) {
      throw new IOException("Cannot write a new block to a closed partition.");
    }

    final PartitionMetadata partitionMetadata =
        new PartitionMetadata(key, partitionSize, writtenBytesCursor, elementsTotal);
    partitionMetadataList.add(partitionMetadata);
    writtenBytesCursor += partitionSize;
  }

  /**
   * Gets a list containing the partition metadata of corresponding block.
   *
   * @return the list containing the partition metadata.
   * @throws IOException if fail to get the iterable.
   */
  public final List<PartitionMetadata<K>> getPartitionMetadataList() throws IOException {
    return Collections.unmodifiableList(partitionMetadataList);
  }

  /**
   * Deletes the metadata.
   *
   * @throws IOException if fail to delete.
   */
  public abstract void deleteMetadata() throws IOException;

  /**
   * Notifies that all writes are finished for the block corresponding to this metadata.
   *
   * @throws IOException if fail to commit.
   */
  public abstract void commitBlock() throws IOException;

  /**
   * Set the commit value.
   * @param committed whether this block is committed or not.
   */
  protected final void setCommitted(final boolean committed) {
    this.committed.set(committed);
  }

  /**
   * @return whether this file is committed or not.
   */
  public final boolean isCommitted() {
    return committed.get();
  }
}
