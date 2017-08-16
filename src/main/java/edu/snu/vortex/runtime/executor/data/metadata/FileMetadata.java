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

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * This class represents a metadata for a (local / remote) file partition.
 */
public abstract class FileMetadata {

  private final boolean hashed; // Each block in the corresponding partition has a single hash value or not.
  private final List<BlockMetadata> blockMetadataList;
  private long position; // How many bytes are (at least, logically) written in the file.

  protected FileMetadata(final boolean hashed) {
    this(hashed, new LinkedList<>());
  }

  protected FileMetadata(final boolean hashed,
                         final List<BlockMetadata> blockMetadataList) {
    this.hashed = hashed;
    this.blockMetadataList = blockMetadataList;
  }

  /**
   * Appends a metadata for a block.
   * This method is not designed for concurrent write.
   * Therefore, it does not do any synchronization and this change will valid in local only.
   * Further synchronization will be done in {@link FileMetadata#getAndSetWritten()} if needed.
   *
   * @param hashValue   of the block.
   * @param blockSize   of the block.
   * @param numElements of the block.
   */
  public final void appendBlockMetadata(final int hashValue,
                                        final int blockSize,
                                        final long numElements) {
    blockMetadataList.add(new BlockMetadata(hashValue, blockSize, position, numElements));
    position += blockSize;
  }

  /**
   * Gets the un-modifiable form of block metadata list.
   *
   * @return the list of block metadata.
   */
  public final List<BlockMetadata> getBlockMetadataList() {
    return Collections.unmodifiableList(blockMetadataList);
  }

  /**
   * Gets whether the whole data for this partition is written or not yet.
   *
   * @return whether the whole data for this partition is written or not yet.
   */
  public abstract boolean isWritten();

  /**
   * Gets the current written flag and marks true if not set yet.
   * This method synchronizes all changes if needed.
   *
   * @return {@code true} if already set, or {@code false} if not.
   * @throws IOException if fail to finish the write.
   */
  public abstract boolean getAndSetWritten() throws IOException;

  /**
   * Gets whether each block in the corresponding partition has a single hash value or not.
   *
   * @return whether each block in the corresponding partition has a single hash value or not.
   */
  public final boolean isHashed() {
    return hashed;
  }

  /**
   * Deletes the metadata.
   * @throws IOException if fail to delete.
   */
  public abstract void deleteMetadata() throws IOException;
}
