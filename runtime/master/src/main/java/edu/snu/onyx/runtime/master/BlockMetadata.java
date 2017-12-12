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
import edu.snu.onyx.runtime.common.comm.ControlMessage;
import edu.snu.onyx.runtime.common.state.BlockState;
import edu.snu.onyx.runtime.common.exception.AbsentBlockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * This class represents a block metadata stored in the metadata server.
 */
@ThreadSafe
final class BlockMetadata {
  // Partition level metadata.
  private static final Logger LOG = LoggerFactory.getLogger(BlockManagerMaster.class.getName());
  private final String blockId;
  private final BlockState blockState;
  private volatile CompletableFuture<String> locationFuture; // the future of the location of this block.

  // Partition level metadata. These information will be managed only for remote blocks.
  private volatile List<PartitionMetadataInServer> partitionMetadataList;
  private volatile long writtenBytesCursor; // How many bytes are (at least, logically) written in the file.
  private volatile int publishedPartitionCursor; // Cursor dividing the published blocks and un-published partitions.

  /**
   * Constructs the metadata for a block.
   *
   * @param blockId the id of the block.
   */
  BlockMetadata(final String blockId) {
    // Initialize block level metadata.
    this.blockId = blockId;
    this.blockState = new BlockState();
    this.locationFuture = new CompletableFuture<>();
    // Initialize block level metadata.
    this.partitionMetadataList = new ArrayList<>();
    this.writtenBytesCursor = 0;
    this.publishedPartitionCursor = 0;
  }

  /**
   * Deals with state change of the corresponding block.
   *
   * @param newState the new state of the block.
   * @param location the location of the block (e.g., worker id, remote store).
   *                 {@code null} if not committed or lost.
   */
  synchronized void onStateChanged(final BlockState.State newState,
                                   @Nullable final String location) {
    final StateMachine stateMachine = blockState.getStateMachine();
    final Enum oldState = stateMachine.getCurrentState();
    LOG.debug("Block State Transition: id {} from {} to {}", new Object[]{blockId, oldState, newState});

    switch (newState) {
      case SCHEDULED:
        stateMachine.setState(newState);
        break;
      case LOST:
        LOG.info("Block {} lost in {}", new Object[]{blockId, location});
      case LOST_BEFORE_COMMIT:
      case REMOVED:
        // Reset the block location and committer information.
        locationFuture.completeExceptionally(new AbsentBlockException(blockId, newState));
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
   * Completes the location future of this block.
   *
   * @param location the location of the block.
   */
  private synchronized void completeLocationFuture(final String location) {
    locationFuture.complete(location);
  }

  /**
   * @return the block id.
   */
  String getBlockId() {
    return blockId;
  }

  /**
   * @return the state of this block.
   */
  BlockState getBlockState() {
    return blockState;
  }

  /**
   * @return the future of the location of this Block.
   */
  synchronized CompletableFuture<String> getLocationFuture() {
    return locationFuture;
  }

  /**
   * Reserves the region for a partition and get the metadata for the partition.
   *
   * @param partitionMetadata the partition metadata to append.
   * @return the pair of the index of reserved block and starting position of the block in the file.
   */
  synchronized Pair<Integer, Long> reservePartition(final ControlMessage.PartitionMetadataMsg partitionMetadata) {
    final int partitionSize = partitionMetadata.getPartitionSize();
    final long currentPosition = writtenBytesCursor;
    final int partitionIdx = partitionMetadataList.size();
    final ControlMessage.PartitionMetadataMsg partitionMetadataToStore =
        ControlMessage.PartitionMetadataMsg.newBuilder()
            .setHashValue(partitionMetadata.getHashValue())
            .setPartitionSize(partitionSize)
            .setOffset(currentPosition)
            .setNumElements(partitionMetadata.getNumElements())
            .build();

    writtenBytesCursor += partitionSize;
    partitionMetadataList.add(new PartitionMetadataInServer(partitionMetadataToStore));
    return Pair.of(partitionIdx, currentPosition);
  }

  /**
   * Notifies that some partitions are written.
   *
   * @param partitionIndicesToCommit the indices of the partitions to commit.
   */
  synchronized void commitPartitions(final Iterable<Integer> partitionIndicesToCommit) {
    // Mark the partitions committed.
    partitionIndicesToCommit.forEach(idx -> partitionMetadataList.get(idx).setCommitted());

    while (publishedPartitionCursor < partitionMetadataList.size()
        && partitionMetadataList.get(publishedPartitionCursor).isCommitted()) {
      // If the first partition in the un-published section is committed, publish it.
      publishedPartitionCursor++;
    }
  }

  /**
   * Removes the partition metadata for the corresponding block.
   */
  synchronized void removePartitionMetadata() {
    this.partitionMetadataList = new ArrayList<>();
    this.writtenBytesCursor = 0;
    this.publishedPartitionCursor = 0;
  }

  /**
   * Gets the list of the partition metadata.
   *
   * @return the list of partition metadata.
   */
  synchronized List<PartitionMetadataInServer> getPartitionMetadataList() {
    return Collections.unmodifiableList(partitionMetadataList.subList(0, publishedPartitionCursor));
  }

  /**
   * The partition metadata in server side.
   * These information will be managed only for remote blocks.
   */
  final class PartitionMetadataInServer {
    private final ControlMessage.PartitionMetadataMsg partitionMetadataMsg;
    private volatile boolean committed;

    private PartitionMetadataInServer(final ControlMessage.PartitionMetadataMsg partitionMetadataMsg) {
      this.partitionMetadataMsg = partitionMetadataMsg;
      this.committed = false;
    }

    private boolean isCommitted() {
      return committed;
    }

    private void setCommitted() {
      committed = true;
    }

    ControlMessage.PartitionMetadataMsg getPartitionMetadataMsg() {
      return partitionMetadataMsg;
    }
  }
}
