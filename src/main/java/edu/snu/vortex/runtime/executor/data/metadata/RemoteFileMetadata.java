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

import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.executor.PersistentConnectionToMaster;
import edu.snu.vortex.runtime.master.RuntimeMaster;

import javax.annotation.concurrent.ThreadSafe;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * This class represents a metadata for a remote file partition.
 * Because the data is stored in a remote file and globally accessed by multiple nodes,
 * each access (create - write - close, read, or deletion) for a partition needs one instance of this metadata.
 * Concurrent write for a single file is supported, but each writer in different executor
 * has to have separate instance of this class.
 * It supports concurrent write for a single partition, but each writer has to have separate instance of this class.
 * These accesses are judiciously synchronized by the metadata server in master.
 */
@ThreadSafe
public final class RemoteFileMetadata extends FileMetadata {

  private final String partitionId;
  private final String executorId;
  private final PersistentConnectionToMaster connectionToMaster;
  private volatile Iterable<BlockMetadata> blockMetadataIterable;

  /**
   * Opens a partition metadata.
   * TODO #410: Implement metadata caching for the RemoteFileMetadata.
   *
   * @param commitPerBlock     whether commit every block write or not.
   * @param partitionId        the id of the partition.
   * @param executorId         the id of the executor.
   * @param connectionToMaster the connection for sending messages to master.
   */
  public RemoteFileMetadata(final boolean commitPerBlock,
                            final String partitionId,
                            final String executorId,
                            final PersistentConnectionToMaster connectionToMaster) {
    super(commitPerBlock);
    this.partitionId = partitionId;
    this.executorId = executorId;
    this.connectionToMaster = connectionToMaster;
  }

  /**
   * Reserves the region for a block and get the metadata for the block.
   *
   * @see FileMetadata#reserveBlock(int, int, long).
   */
  @Override
  public synchronized BlockMetadata reserveBlock(final int hashValue,
                                                 final int blockSize,
                                                 final long elementsTotal) throws IOException {
    // Convert the block metadata to a block metadata message (without offset).
    final ControlMessage.BlockMetadataMsg blockMetadataMsg =
        ControlMessage.BlockMetadataMsg.newBuilder()
            .setHashValue(hashValue)
            .setBlockSize(blockSize)
            .setNumElements(elementsTotal)
            .build();

    // Send the block metadata to the metadata server in the master and ask where to store the block.
    final CompletableFuture<ControlMessage.Message> reserveBlockResponseFuture =
        connectionToMaster.getMessageSender().request(
            ControlMessage.Message.newBuilder()
                .setId(RuntimeIdGenerator.generateMessageId())
                .setType(ControlMessage.MessageType.ReserveBlock)
                .setReserveBlockMsg(
                    ControlMessage.ReserveBlockMsg.newBuilder()
                        .setExecutorId(executorId)
                        .setPartitionId(partitionId)
                        .setBlockMetadata(blockMetadataMsg))
                .build());

    // Get the response from the metadata server.
    final ControlMessage.Message responseFromMaster;
    try {
      responseFromMaster = reserveBlockResponseFuture.get();
    } catch (final InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }

    assert (responseFromMaster.getType() == ControlMessage.MessageType.ReserveBlockResponse);
    final ControlMessage.ReserveBlockResponseMsg reserveBlockResponseMsg =
        responseFromMaster.getReserveBlockResponseMsg();
    if (!reserveBlockResponseMsg.hasPositionToWrite()) {
      // TODO #463: Support incremental read. Check whether this partition is committed in the metadata server side.
      throw new IOException("Cannot append the block metadata.");
    }
    final int blockIndex = reserveBlockResponseMsg.getBlockIdx();
    final long positionToWrite = reserveBlockResponseMsg.getPositionToWrite();
    return new BlockMetadata(blockIndex, hashValue, blockSize, positionToWrite, elementsTotal);
  }

  /**
   * Notifies that some blocks are written.
   *
   * @see FileMetadata#commitBlocks(Iterable).
   */
  @Override
  public synchronized void commitBlocks(final Iterable<BlockMetadata> blockMetadataToCommit) {
    final List<Integer> blockIndices = new ArrayList<>();
    blockMetadataToCommit.forEach(blockMetadata -> {
      blockMetadata.setCommitted();
      blockIndices.add(blockMetadata.getBlockIdx());
    });

    // Notify that these blocks are committed to the metadata server.
    connectionToMaster.getMessageSender().send(
        ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setType(ControlMessage.MessageType.CommitMetadata)
            .setCommitMetadataMsg(
                ControlMessage.CommitMetadataMsg.newBuilder()
                    .setPartitionId(partitionId)
                    .addAllBlockIdx(blockIndices))
            .build());
  }

  /**
   * Gets a iterable containing the block metadata of corresponding partition.
   *
   * @see FileMetadata#getBlockMetadataIterable().
   */
  @Override
  public synchronized Iterable<BlockMetadata> getBlockMetadataIterable() throws IOException {
    if (blockMetadataIterable == null) {
      blockMetadataIterable = getBlockMetadataFromServer();
    }
    return blockMetadataIterable;
  }

  /**
   * @see FileMetadata#deleteMetadata().
   */
  @Override
  public void deleteMetadata() throws IOException {
    connectionToMaster.getMessageSender().send(
        ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setType(ControlMessage.MessageType.RemoveMetadata)
            .setRemoveMetadataMsg(
                ControlMessage.RemoveMetadataMsg.newBuilder()
                    .setPartitionId(partitionId))
            .build());
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

  /**
   * Gets the iterable of block metadata from the metadata server.
   * If write for this partition is not ended, the metadata Fserver will publish the committed blocks to this iterable.
   *
   * @return the received file metadata.
   * @throws IOException if fail to get the metadata.
   */
  private Iterable<BlockMetadata> getBlockMetadataFromServer() throws IOException {
    final List<BlockMetadata> blockMetadataList = new ArrayList<>();

    // Ask the metadata server in the master for the metadata
    final CompletableFuture<ControlMessage.Message> metadataResponseFuture =
        connectionToMaster.getMessageSender().request(
            ControlMessage.Message.newBuilder()
                .setId(RuntimeIdGenerator.generateMessageId())
                .setType(ControlMessage.MessageType.RequestMetadata)
                .setRequestMetadataMsg(
                    ControlMessage.RequestMetadataMsg.newBuilder()
                        .setExecutorId(executorId)
                        .setPartitionId(partitionId)
                        .build())
                .build());

    final ControlMessage.Message responseFromMaster;
    try {
      responseFromMaster = metadataResponseFuture.get();
    } catch (final InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }

    assert (responseFromMaster.getType() == ControlMessage.MessageType.MetadataResponse);
    final ControlMessage.MetadataResponseMsg metadataResponseMsg = responseFromMaster.getMetadataResponseMsg();
    if (metadataResponseMsg.hasState()) {
      // Response has an exception state.
      throw new IOException(new Throwable(
          "Cannot get the metadata of partition " + partitionId + " from the metadata server: "
              + "The partition state is " + RuntimeMaster.convertPartitionState(metadataResponseMsg.getState())));
    }

    // Construct the metadata from the response.
    final List<ControlMessage.BlockMetadataMsg> blockMetadataMsgList = metadataResponseMsg.getBlockMetadataList();
    for (int blockIdx = 0; blockIdx < blockMetadataMsgList.size(); blockIdx++) {
      final ControlMessage.BlockMetadataMsg blockMetadataMsg = blockMetadataMsgList.get(blockIdx);
      if (!blockMetadataMsg.hasOffset()) {
        throw new IOException(new Throwable(
            "The metadata of a block in the " + partitionId + " does not have offset value."));
      }
      blockMetadataList.add(new BlockMetadata(
          blockIdx,
          blockMetadataMsg.getHashValue(),
          blockMetadataMsg.getBlockSize(),
          blockMetadataMsg.getOffset(),
          blockMetadataMsg.getNumElements()
      ));
    }

    // TODO #463: Support incremental read. Return a "ClosableBlockingIterable".
    return blockMetadataList;
  }
}
