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
import edu.snu.vortex.runtime.exception.UnsupportedMethodException;
import edu.snu.vortex.runtime.executor.PersistentConnectionToMaster;
import edu.snu.vortex.runtime.master.RuntimeMaster;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * This class represents a metadata for a remote file partition.
 * Because the data is stored in a remote file and globally accessed by multiple nodes,
 * each access (create - write - close, read, or deletion) for a partition needs one instance of this metadata.
 * TODO #355: Support I-file write.
 * It supports concurrent write for a single partition, but each writer has to have separate instance of this class.
 * These accesses are judiciously synchronized by the metadata server in master.
 */
public final class RemoteFileMetadata extends FileMetadata {

  private boolean written; // The whole data for this partition is written or not yet.
  private String partitionId;
  private final PersistentConnectionToMaster connectionToMaster;

  /**
   * Creates a new partition metadata to write.
   *
   * @param hashed             each block has a single hash value or not.
   * @param partitionId        the id of the partition.
   * @param connectionToMaster the connection for sending messages to master.
   */
  private RemoteFileMetadata(final boolean hashed,
                             final String partitionId,
                             final PersistentConnectionToMaster connectionToMaster) {
    super(hashed);
    this.written = false;
    this.partitionId = partitionId;
    this.connectionToMaster = connectionToMaster;
  }

  /**
   * Opens an exist metadata to read.
   *
   * @param hashed             each block has a single hash value or not.
   * @param partitionId        the id of the partition.
   * @param blockMetadataList  the list of block metadata.
   * @param connectionToMaster the connection for sending messages to master.
   */
  private RemoteFileMetadata(final boolean hashed,
                             final String partitionId,
                             final List<BlockMetadata> blockMetadataList,
                             final PersistentConnectionToMaster connectionToMaster) {
    super(hashed, blockMetadataList);
    this.written = true;
    this.partitionId = partitionId;
    this.connectionToMaster = connectionToMaster;
  }

  /**
   * Reserves a region for storing a block and appends a metadata for the block.
   * This method is designed for concurrent write.
   * Therefore, it will communicate with the metadata server and synchronize the write.
   *
   * @param hashValue   of the block.
   * @param blockSize   of the block.
   * @param numElements of the block.
   */
  public void reserveBlock(final int hashValue,
                           final int blockSize,
                           final long numElements) {
    // TODO #355: Support I-file write.
    throw new UnsupportedMethodException("reserveBlock(...) is not supported yet.");
  }

  /**
   * Marks that the whole data for this partition is written.
   * This method synchronizes all changes if needed.
   *
   * @return {@code true} if already set, or {@code false} if not.
   * @throws IOException if fail to finish the write.
   */
  @Override
  public boolean getAndSetWritten() throws IOException {
    if (written) {
      return true;
    }
    written = true;

    // Send the whole metadata to the metadata server.
    final List<ControlMessage.BlockMetadataMsg> blockMetadataMsgList = new LinkedList<>();
    for (final BlockMetadata blockMetadata : getBlockMetadataList()) {
      // Convert the block metadata to a block metadata message.
      blockMetadataMsgList.add(
          ControlMessage.BlockMetadataMsg.newBuilder()
              .setHashValue(blockMetadata.getHashValue())
              .setBlockSize(blockMetadata.getBlockSize())
              .setOffset(blockMetadata.getOffset())
              .setNumElements(blockMetadata.getNumElements())
              .build()
      );
    }
    connectionToMaster.getMessageSender().send(
        ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setType(ControlMessage.MessageType.StoreMetadata)
            .setStoreMetadataMsg(
                ControlMessage.StoreMetadataMsg.newBuilder()
                    .setPartitionId(partitionId)
                    .setHashed(isHashed())
                    .addAllBlockMetadata(blockMetadataMsgList))
            .build());

    return false;
  }

  /**
   * Gets whether the whole data for this partition is written or not yet.
   *
   * @return whether the whole data for this partition is written or not yet.
   */
  @Override
  public boolean isWritten() {
    return written;
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
   * Creates a file metadata for a partition in the remote storage to write.
   * The corresponding {@link FileMetadata#getAndSetWritten()}} for the returned metadata is required.
   *
   * @param partitionId        the id of the partition.
   * @param hashed             whether each block in this partition has a single hash value or not.
   * @param connectionToMaster the connection for sending messages to master.
   * @return the created file metadata.
   */
  public static RemoteFileMetadata create(final String partitionId,
                                          final boolean hashed,
                                          final PersistentConnectionToMaster connectionToMaster) {
    return new RemoteFileMetadata(hashed, partitionId, connectionToMaster);
  }

  /**
   * Gets the corresponding file metadata for a partition in the remote storage to read.
   * It will communicates with the metadata server to get the metadata.
   *
   * @param partitionId        the id of the partition.
   * @param executorId         the id of the executor.
   * @param connectionToMaster the connection for sending messages to master.
   * @return the read file metadata.
   * @throws IOException if fail to read the metadata.
   * @throws InterruptedException if interrupted during waiting the response from the metadata server.
   * @throws ExecutionException if the request to the metadata server completed exceptionally.
   */
  public static RemoteFileMetadata get(final String partitionId,
                                       final String executorId,
                                       final PersistentConnectionToMaster connectionToMaster)
      throws IOException, InterruptedException, ExecutionException {
    final List<BlockMetadata> blockMetadataList = new ArrayList<>();
    // TODO #410: Implement metadata caching for the RemoteFileMetadata.

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

    final ControlMessage.Message responseFromMaster = metadataResponseFuture.get();
    assert (responseFromMaster.getType() == ControlMessage.MessageType.MetadataResponse);
    final ControlMessage.MetadataResponseMsg metadataResponseMsg = responseFromMaster.getMetadataResponseMsg();
    if (!metadataResponseMsg.hasHashed()) {
      // Response does not have any metadata.
      if (metadataResponseMsg.hasState()) {
        throw new IOException(new Throwable(
            "Cannot get the metadata of partition " + partitionId + " from the metadata server: "
                + "The partition state is " + RuntimeMaster.convertPartitionState(metadataResponseMsg.getState())));
      } else {
        throw new IOException(new Throwable(
            "Cannot get the metadata of partition " + partitionId + " from the metadata server: "
                + "The partition is committed but the metadata does not exist"));
      }
    }

    // Construct the metadata from the response.
    final boolean hashed = metadataResponseMsg.getHashed();
    final List<ControlMessage.BlockMetadataMsg> blockMetadataMsgList = metadataResponseMsg.getBlockMetadataList();
    for (final ControlMessage.BlockMetadataMsg blockMetadataMsg : blockMetadataMsgList) {
      blockMetadataList.add(new BlockMetadata(
          blockMetadataMsg.getHashValue(),
          blockMetadataMsg.getBlockSize(),
          blockMetadataMsg.getOffset(),
          blockMetadataMsg.getNumElements()
      ));
    }

    return new RemoteFileMetadata(hashed, partitionId, blockMetadataList, connectionToMaster);
  }
}
