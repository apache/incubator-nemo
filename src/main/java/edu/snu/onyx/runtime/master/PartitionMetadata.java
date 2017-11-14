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
package edu.snu.onyx.runtime.master;

import edu.snu.onyx.common.Pair;
import edu.snu.onyx.common.StateMachine;
import edu.snu.onyx.runtime.common.grpc.Common;
import edu.snu.onyx.runtime.common.state.PartitionState;
import edu.snu.onyx.runtime.exception.AbsentPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * This class represents a partition metadata stored in the metadata server.
 */
@ThreadSafe
final class PartitionMetadata {
  // Partition level metadata.
  private static final Logger LOG = LoggerFactory.getLogger(PartitionManagerMaster.class.getName());
  private final String partitionId;
  private final PartitionState partitionState;
  // TODO #446: Control the Point of Partition Fetch in Executor.
  private volatile CompletableFuture<String> locationFuture; // the future of the location of this block.

  // Block level metadata. These information will be managed only for remote partitions.
  private volatile List<BlockMetadataInServer> blockMetadataList;
  private volatile long writtenBytesCursor; // How many bytes are (at least, logically) written in the file.
  private volatile int publishedBlockCursor; // Cursor dividing the published blocks and un-published blocks.

  /**
   * Constructs the metadata for a partition.
   *
   * @param partitionId         the id of the partition.
   */
  PartitionMetadata(final String partitionId) {
    // Initialize partition level metadata.
    this.partitionId = partitionId;
    this.partitionState = new PartitionState();
    this.locationFuture = new CompletableFuture<>();
    // Initialize block level metadata.
    this.blockMetadataList = new ArrayList<>();
    this.writtenBytesCursor = 0;
    this.publishedBlockCursor = 0;
  }

  /**
   * Deals with state change of the corresponding partition.
   *
   * @param newState         the new state of the partition.
   * @param location         the location of the partition (e.g., worker id, remote store).
   *                         {@code null} if not committed or lost.
   */
  synchronized void onStateChanged(final PartitionState.State newState,
                                   @Nullable final String location) {
    final StateMachine stateMachine = partitionState.getStateMachine();
    final Enum oldState = stateMachine.getCurrentState();
    LOG.debug("Partition State Transition: id {} from {} to {}", new Object[]{partitionId, oldState, newState});

    switch (newState) {
      case SCHEDULED:
        stateMachine.setState(newState);
        break;
      case LOST:
        LOG.info("Partition {} lost in {}", new Object[]{partitionId, location});
      case LOST_BEFORE_COMMIT:
      case REMOVED:
        // Reset the partition location and committer information.
        locationFuture.completeExceptionally(new AbsentPartitionException(partitionId, newState));
        locationFuture = new CompletableFuture<>();
        stateMachine.setState(newState);
        break;
      case COMMITTED:
        assert (location != null);
        completeLocationFuture(location);
        stateMachine.setState(newState);
        break;
      default:
        throw new UnsupportedOperationException(newState.toString());
    }
  }

  /**
   * Completes the location future of this partition.
   *
   * @param location the location of the partition.
   */
  private synchronized void completeLocationFuture(final String location) {
    locationFuture.complete(location);
  }

  /**
   * @return the partition id.
   */
  String getPartitionId() {
    return partitionId;
  }

  /**
   * @return the state of this partition.
   */
  PartitionState getPartitionState() {
    return partitionState;
  }

  /**
   * @return the future of the location of this partition.
   */
  synchronized CompletableFuture<String> getLocationFuture() {
    return locationFuture;
  }

  /**
   * Reserves the region for a block and get the metadata for the block.
   *
   * @param blockMetadata the block metadata to append.
   * @return the pair of the index of reserved block and starting position of the block in the file.
   */
  synchronized Pair<Integer, Long> reserveBlock(final Common.BlockMetadata blockMetadata) {
    final int blockSize = blockMetadata.getBlockSize();
    final long currentPosition = writtenBytesCursor;
    final int blockIdx = blockMetadataList.size();
    final Common.BlockMetadata blockMetadataToStore =
        Common.BlockMetadata.newBuilder()
            .setHashValue(blockMetadata.getHashValue())
            .setBlockSize(blockSize)
            .setOffset(currentPosition)
            .setNumElements(blockMetadata.getNumElements())
            .build();

    writtenBytesCursor += blockSize;
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

    while (publishedBlockCursor < blockMetadataList.size()
        && blockMetadataList.get(publishedBlockCursor).isCommitted()) {
      // If the first block in the un-published section is committed, publish it.
      // TODO #463: Support incremental read. Send the newly committed blocks to subscribers.
      publishedBlockCursor++;
    }
  }

  /**
   * Removes the block metadata for the corresponding partition.
   */
  synchronized void removeBlockMetadata() {
    this.blockMetadataList = new ArrayList<>();
    this.publishedBlockCursor = 0;
  }

  /**
   * Gets the list of the block metadata.
   *
   * @return the list of block metadata.
   */
  synchronized List<BlockMetadataInServer> getBlockMetadataList() {
    // TODO #463: Support incremental read. Add the requester to the subscriber list.
    return Collections.unmodifiableList(blockMetadataList.subList(0, publishedBlockCursor));
  }

  /**
   * The block metadata in server side.
   * These information will be managed only for remote partitions.
   */
  final class BlockMetadataInServer {
    private final Common.BlockMetadata blockMetadata;
    private volatile boolean committed;

    private BlockMetadataInServer(final Common.BlockMetadata blockMetadata) {
      this.blockMetadata = blockMetadata;
      this.committed = false;
    }

    private boolean isCommitted() {
      return committed;
    }

    private void setCommitted() {
      committed = true;
    }

    Common.BlockMetadata getBlockMetadata() {
      return blockMetadata;
    }
  }
}
