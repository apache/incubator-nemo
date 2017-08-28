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
package edu.snu.vortex.runtime.master.metadata;

import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.exception.IllegalMessageException;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * This class represents a metadata stored in the metadata server.
 * TODO #430: Handle Concurrency at Partition Level.
 * TODO #431: Include Partition Metadata in a Partition.
 */
@NotThreadSafe
final class MetadataInServer {

  private final boolean hashed; // Each block in the corresponding file has a single hash value or not.
  private final List<ControlMessage.BlockMetadataMsg> blockMetadataList;
  private final boolean appendable;
  private long position; // How many bytes are (at least, logically) written in the file.

  /**
   * Constructs a appendable metadata.
   *
   * @param hashed whether each block in the corresponding file has a single hash value or not.
   */
  MetadataInServer(final boolean hashed) {
    this.hashed = hashed;
    this.appendable = true;
    this.blockMetadataList = new LinkedList<>();
    this.position = 0;
  }

  /**
   * Constructs a non-appendable metadata.
   *
   * @param hashed whether each block in the corresponding file has a single hash value or not.
   * @param blockMetadataList the list of block metadata.
   */
  MetadataInServer(final boolean hashed,
                   final List<ControlMessage.BlockMetadataMsg> blockMetadataList) {
    this.hashed = hashed;
    this.appendable = false;
    this.blockMetadataList = blockMetadataList;
    this.position = 0;
  }

  /**
   * Appends a block metadata.
   *
   * @param blockMetadata the block metadata to append.
   * @return the starting position of the block in the file.
   * @throws IllegalMessageException if fail to append the block metadata.
   */
  long appendBlockMetadata(final ControlMessage.BlockMetadataMsg blockMetadata) throws IllegalMessageException {
    if (!appendable) {
      throw new IllegalMessageException(new Throwable("Cannot append a block metadata to this partition."));
    }
    final int blockSize = blockMetadata.getBlockSize();
    final long currentPosition = position;
    final ControlMessage.BlockMetadataMsg blockMetadataToStore =
        ControlMessage.BlockMetadataMsg.newBuilder()
            .setHashValue(blockMetadata.getHashValue())
            .setBlockSize(blockSize)
            .setOffset(currentPosition)
            .setNumElements(blockMetadata.getNumElements())
            .build();

    position += blockSize;
    blockMetadataList.add(blockMetadataToStore);
    return currentPosition;
  }

  /**
   * Gets the list of the block metadata.
   *
   * @return the list of block metadata.
   */
  List<ControlMessage.BlockMetadataMsg> getBlockMetadataList() {
    return Collections.unmodifiableList(blockMetadataList);
  }

  /**
   * Gets whether each block in the corresponding partition has a single hash value or not.
   *
   * @return whether each block in the corresponding partition has a single hash value or not.
   */
  boolean isHashed() {
    return hashed;
  }
}
