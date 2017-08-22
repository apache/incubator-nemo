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

import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageContext;
import edu.snu.vortex.runtime.exception.AbsentPartitionException;
import edu.snu.vortex.runtime.master.PartitionManagerMaster;
import edu.snu.vortex.runtime.master.RuntimeMaster;
import org.apache.reef.tang.InjectionFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Manages the metadata of remote partitions.
 * For now, all its operations are synchronized to guarantee thread safety.
 * TODO #430: Handle Concurrency at Partition Level.
 * TODO #431: Include Partition Metadata in a Partition.
 */
@ThreadSafe
public final class MetadataManager {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataManager.class.getName());
  private final Map<String, MetadataInServer> partitionIdToMetadata;
  private final InjectionFuture<PartitionManagerMaster> partitionManagerMaster;

  @Inject
  private MetadataManager(final InjectionFuture<PartitionManagerMaster> partitionManagerMaster) {
    this.partitionIdToMetadata = new HashMap<>();
    this.partitionManagerMaster = partitionManagerMaster;
  }

  /**
   * Stores a new (whole) metadata for a remote partition.
   *
   * @param message the message having metadata to store.
   */
  public synchronized void onStoreMetadata(final ControlMessage.Message message) {
    assert (message.getType() == ControlMessage.MessageType.StoreMetadata);
    final ControlMessage.StoreMetadataMsg storeMsg = message.getStoreMetadataMsg();
    final String partitionId = storeMsg.getPartitionId();
    final boolean hashed = storeMsg.getHashed();
    final List<ControlMessage.BlockMetadataMsg> blockMetadataList = storeMsg.getBlockMetadataList();
    final MetadataInServer previousMetadata =
        partitionIdToMetadata.putIfAbsent(partitionId, new MetadataInServer(hashed, blockMetadataList));
    if (previousMetadata != null) {
      LOG.error("Metadata for {} already exists. It will be replaced.", partitionId);
    }
  }

  /**
   * Accepts a request for a metadata and replies with the metadata for a remote partition.
   *
   * @param message        the message having metadata to store.
   * @param messageContext the context to reply.
   */
  public synchronized void onRequestMetadata(final ControlMessage.Message message,
                                             final MessageContext messageContext) {
    assert (message.getType() == ControlMessage.MessageType.RequestMetadata);
    final ControlMessage.RequestMetadataMsg requestMsg = message.getRequestMetadataMsg();
    final String partitionId = requestMsg.getPartitionId();
    // Check whether the partition is committed. The actual location is not important.
    final CompletableFuture<String> locationFuture =
        partitionManagerMaster.get().getPartitionLocationFuture(partitionId);

    locationFuture.whenComplete((location, throwable) -> {
      final ControlMessage.MetadataResponseMsg.Builder responseBuilder =
          ControlMessage.MetadataResponseMsg.newBuilder()
              .setRequestId(message.getId());
      if (throwable == null) {
        // Well committed.
        final MetadataInServer metadata = partitionIdToMetadata.get(partitionId);
        if (metadata != null) {
          responseBuilder.setHashed(metadata.isHashed());
          responseBuilder.addAllBlockMetadata(metadata.getBlockMetadataList());
        } else {
          LOG.error("Metadata for {} dose not exist. Failed to get it.", partitionId);
        }
      } else {
        responseBuilder.setState(
            RuntimeMaster.convertPartitionState(((AbsentPartitionException) throwable).getState()));
      }
      messageContext.reply(
          ControlMessage.Message.newBuilder()
              .setId(RuntimeIdGenerator.generateMessageId())
              .setType(ControlMessage.MessageType.MetadataResponse)
              .setMetadataResponseMsg(responseBuilder.build())
              .build());
    });
  }

  /**
   * Removes the metadata for a remote partition.
   *
   * @param message the message pointing the metadata to remove.
   */
  public synchronized void onRemoveMetadata(final ControlMessage.Message message) {
    assert (message.getType() == ControlMessage.MessageType.RemoveMetadata);
    final ControlMessage.RemoveMetadataMsg removeMsg = message.getRemoveMetadataMsg();
    final String partitionId = removeMsg.getPartitionId();

    final boolean removed = partitionIdToMetadata.remove(partitionId) != null;
    if (!removed) {
      LOG.error("Metadata for {} dose not exist. Failed to delete it.", partitionId);
    }
  }
}
