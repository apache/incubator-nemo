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

import com.google.protobuf.ByteString;
import edu.snu.onyx.runtime.common.RuntimeIdGenerator;
import edu.snu.onyx.runtime.common.comm.ControlMessage;
import edu.snu.onyx.runtime.common.message.MessageEnvironment;
import edu.snu.onyx.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.SerializationUtils;

import javax.annotation.concurrent.ThreadSafe;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * This class represents a metadata for a remote file block.
 * Because the data is stored in a remote file and globally accessed by multiple nodes,
 * each access (create - write - close, read, or deletion) for a block needs one instance of this metadata.
 * These accesses are judiciously synchronized by the metadata server in master.
 * @param <K> the key type of its partitions.
 */
@ThreadSafe
public final class RemoteFileMetadata<K extends Serializable> extends FileMetadata<K> {

  private final String blockId;
  private final String executorId;
  private final PersistentConnectionToMasterMap connectionToMaster;
  private volatile Iterable<PartitionMetadata<K>> partitionMetadataIterable;

  /**
   * Opens a block metadata.
   *
   * @param commitPerBlock     whether commit every block write or not.
   * @param blockId            the id of the block.
   * @param executorId         the id of the executor.
   * @param connectionToMaster the connection for sending messages to master.
   */
  public RemoteFileMetadata(final boolean commitPerBlock,
                            final String blockId,
                            final String executorId,
                            final PersistentConnectionToMasterMap connectionToMaster) {
    super(commitPerBlock);
    this.blockId = blockId;
    this.executorId = executorId;
    this.connectionToMaster = connectionToMaster;
  }

  /**
   * Reserves the region for a partition and get the metadata for the partition.
   *
   * @see FileMetadata#reservePartition(K, int, long).
   */
  @Override
  public synchronized PartitionMetadata reservePartition(final K key,
                                                         final int partitionSize,
                                                         final long elementsTotal) throws IOException {
    // Convert the block metadata to a block metadata message (without offset).
    final ControlMessage.PartitionMetadataMsg partitionMetadataMsg =
        ControlMessage.PartitionMetadataMsg.newBuilder()
            .setKey(ByteString.copyFrom(SerializationUtils.serialize(key)))
            .setPartitionSize(partitionSize)
            .setNumElements(elementsTotal)
            .build();

    // Send the partition metadata to the metadata server in the master and ask where to store the partition.
    final CompletableFuture<ControlMessage.Message> reservePartitionResponseFuture =
        connectionToMaster.getMessageSender(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID).request(
            ControlMessage.Message.newBuilder()
                .setId(RuntimeIdGenerator.generateMessageId())
                .setListenerId(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID)
                .setType(ControlMessage.MessageType.ReservePartition)
                .setReservePartitionMsg(
                    ControlMessage.ReservePartitionMsg.newBuilder()
                        .setExecutorId(executorId)
                        .setBlockId(blockId)
                        .setPartitionMetadata(partitionMetadataMsg))
                .build());

    // Get the response from the metadata server.
    final ControlMessage.Message responseFromMaster;
    try {
      responseFromMaster = reservePartitionResponseFuture.get();
    } catch (final InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }

    assert (responseFromMaster.getType() == ControlMessage.MessageType.ReservePartitionResponse);
    final ControlMessage.ReservePartitionResponseMsg reservePartitionResponseMsg =
        responseFromMaster.getReservePartitionResponseMsg();
    if (!reservePartitionResponseMsg.hasPositionToWrite()) {
      throw new IOException("Cannot append the block metadata.");
    }
    final int partitionIdx = reservePartitionResponseMsg.getPartitionIdx();
    final long positionToWrite = reservePartitionResponseMsg.getPositionToWrite();
    return new PartitionMetadata(partitionIdx, key, partitionSize, positionToWrite, elementsTotal);
  }

  /**
   * Notifies that some partitions are written.
   *
   * @see FileMetadata#commitPartitions(Iterable).
   */
  @Override
  public synchronized void commitPartitions(final Iterable<PartitionMetadata> partitionMetadataToCommit) {
    final List<Integer> partitionIndices = new ArrayList<>();
    partitionMetadataToCommit.forEach(partitionMetadata -> {
      partitionMetadata.setCommitted();
      partitionIndices.add(partitionMetadata.getPartitionIdx());
    });

    // Notify that these partitions are committed to the metadata server.
    connectionToMaster.getMessageSender(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID).send(
        ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setListenerId(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.CommitPartition)
            .setCommitPartitionMsg(
                ControlMessage.CommitPartitionMsg.newBuilder()
                    .setBlockId(blockId)
                    .addAllPartitionIdx(partitionIndices))
            .build());
  }

  /**
   * Gets a iterable containing the partition metadata of corresponding blocks.
   *
   * @see FileMetadata#getPartitionMetadataIterable().
   */
  @Override
  public synchronized Iterable<PartitionMetadata<K>> getPartitionMetadataIterable() throws IOException {
    if (partitionMetadataIterable == null) {
      partitionMetadataIterable = getPartitionMetadataFromServer();
    }
    return partitionMetadataIterable;
  }

  /**
   * @see FileMetadata#deleteMetadata().
   */
  @Override
  public void deleteMetadata() throws IOException {
    connectionToMaster.getMessageSender(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID).send(
        ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setListenerId(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.RemovePartitionMetadata)
            .setRemovePartitionMetadataMsg(
                ControlMessage.RemovePartitionMetadataMsg.newBuilder()
                    .setBlockId(blockId))
            .build());
  }

  /**
   * Notifies that all writes are finished for the block corresponding to this metadata.
   */
  @Override
  public synchronized void commitBlock() {
    // Handled by block manager master (via block commit message).
  }

  /**
   * Gets the iterable of partition metadata from the metadata server.
   *
   * @return the received file metadata.
   * @throws IOException if fail to get the metadata.
   */
  private Iterable<PartitionMetadata<K>> getPartitionMetadataFromServer() throws IOException {
    final List<PartitionMetadata<K>> partitionMetadataList = new ArrayList<>();

    // Ask the metadata server in the master for the metadata
    final CompletableFuture<ControlMessage.Message> metadataResponseFuture =
        connectionToMaster.getMessageSender(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID).request(
            ControlMessage.Message.newBuilder()
                .setId(RuntimeIdGenerator.generateMessageId())
                .setListenerId(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID)
                .setType(ControlMessage.MessageType.RequestPartitionMetadata)
                .setRequestPartitionMetadataMsg(
                    ControlMessage.RequestPartitionMetadataMsg.newBuilder()
                        .setExecutorId(executorId)
                        .setBlockId(blockId)
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
          "Cannot get the metadata of block " + blockId + " from the metadata server: "
              + "The block state is " + metadataResponseMsg.getState()));
    }

    // Construct the metadata from the response.
    final List<ControlMessage.PartitionMetadataMsg> partitionMetadataMsgList =
        metadataResponseMsg.getPartitionMetadataList();
    for (int partitionIdx = 0; partitionIdx < partitionMetadataMsgList.size(); partitionIdx++) {
      final ControlMessage.PartitionMetadataMsg partitionMetadataMsg = partitionMetadataMsgList.get(partitionIdx);
      if (!partitionMetadataMsg.hasOffset()) {
        throw new IOException(new Throwable(
            "The metadata of a partition in the " + blockId + " does not have offset value."));
      }
      partitionMetadataList.add(new PartitionMetadata(
          partitionIdx,
          SerializationUtils.deserialize(partitionMetadataMsg.getKey().toByteArray()),
          partitionMetadataMsg.getPartitionSize(),
          partitionMetadataMsg.getOffset(),
          partitionMetadataMsg.getNumElements()
      ));
    }

    return partitionMetadataList;
  }
}
