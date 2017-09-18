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

import edu.snu.vortex.common.Pair;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.exception.IllegalMessageException;

import javax.annotation.concurrent.ThreadSafe;
import java.util.*;

/**
 * This class represents a metadata stored in the metadata server.
 * TODO #430: Handle Concurrency at Partition Level.
 * TODO #431: Include Partition Metadata in a Partition.
 */
@ThreadSafe
final class MetadataInServer {

  private final List<BlockMetadataInServer> blockMetadataList;
  private volatile long position; // How many bytes are (at least, logically) written in the file.
  private volatile int publishCursor; // Cursor dividing the published blocks and un-published blocks.

  /**
   * Constructs the metadata for a remote partition.
   */
  MetadataInServer() {
    this.blockMetadataList = new ArrayList<>();
    this.position = 0;
    this.publishCursor = 0;
  }

  /**
   * Reserves the region for a block and get the metadata for the block.
   *
   * @param blockMetadata the block metadata to append.
   * @return the pair of the index of reserved block and starting position of the block in the file.
   * @throws IllegalMessageException if fail to append the block metadata.
   */
  synchronized Pair<Integer, Long> reserveBlock(final ControlMessage.BlockMetadataMsg blockMetadata)
      throws IllegalMessageException {
    final int blockSize = blockMetadata.getBlockSize();
    final long currentPosition = position;
    final int blockIdx = blockMetadataList.size();
    final ControlMessage.BlockMetadataMsg blockMetadataToStore =
        ControlMessage.BlockMetadataMsg.newBuilder()
            .setHashValue(blockMetadata.getHashValue())
            .setBlockSize(blockSize)
            .setOffset(currentPosition)
            .setNumElements(blockMetadata.getNumElements())
            .build();

    position += blockSize;
    blockMetadataList.add(new BlockMetadataInServer(blockMetadataToStore));
    return Pair.of(blockIdx, currentPosition);
  }

  /**
   * Notifies that some blocks are written.
   *
   * @param blockIndicesToCommit the indices of the blocks to commit.
   */
  synchronized void commitBlocks(final Iterable<Integer> blockIndicesToCommit) {
    // Mark the blocks committed.
    blockIndicesToCommit.forEach(idx -> blockMetadataList.get(idx).setCommitted());

    while (publishCursor < blockMetadataList.size() && blockMetadataList.get(publishCursor).isCommitted()) {
      // If the first block in the un-published section is committed, publish it.
      // TODO #463: Support incremental read. Send the newly committed blocks to subscribers.
      publishCursor++;
    }
  }

  /**
   * Gets the list of the block metadata.
   *
   * @return the list of block metadata.
   */
  synchronized List<BlockMetadataInServer> getBlockMetadataList() {
    // TODO #463: Support incremental read. Add the requester to the subscriber list.
    return Collections.unmodifiableList(blockMetadataList.subList(0, publishCursor));
  }

  /**
   * The block metadata in server side.
   */
  final class BlockMetadataInServer {
    private final ControlMessage.BlockMetadataMsg blockMetadataMsg;
    private volatile boolean committed;

    BlockMetadataInServer(final ControlMessage.BlockMetadataMsg blockMetadataMsg) {
      this.blockMetadataMsg = blockMetadataMsg;
      this.committed = false;
    }

    public boolean isCommitted() {
      return committed;
    }

    public void setCommitted() {
      committed = true;
    }

    public ControlMessage.BlockMetadataMsg getBlockMetadataMsg() {
      return blockMetadataMsg;
    }
  }
}
