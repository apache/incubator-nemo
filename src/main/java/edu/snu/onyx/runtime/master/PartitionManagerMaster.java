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

import com.google.common.annotations.VisibleForTesting;
import edu.snu.onyx.common.Pair;
import edu.snu.onyx.runtime.common.RuntimeIdGenerator;
import edu.snu.onyx.runtime.common.comm.ControlMessage;
import edu.snu.onyx.runtime.common.message.MessageContext;
import edu.snu.onyx.runtime.common.message.MessageEnvironment;
import edu.snu.onyx.runtime.common.message.MessageListener;
import edu.snu.onyx.runtime.common.state.PartitionState;
import edu.snu.onyx.runtime.exception.AbsentPartitionException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.snu.onyx.runtime.exception.IllegalMessageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static edu.snu.onyx.runtime.common.state.PartitionState.State.SCHEDULED;
import static edu.snu.onyx.runtime.master.RuntimeMaster.convertPartitionState;

/**
 * Master-side partition manager.
 * TODO #433: Reconsider fault tolerance for partitions in remote storage.
 * TODO #493: Detach partitioning from writing.
 */
@ThreadSafe
public final class PartitionManagerMaster {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionManagerMaster.class.getName());
  private final Map<String, PartitionMetadata> partitionIdToMetadata;
  private final Map<String, Set<String>> producerTaskGroupIdToPartitionIds;
  // A lock that can be acquired exclusively or not.
  // Because the PartitionMetadata itself is sufficiently synchronized,
  // operation that runs in a single partition can just acquire a (sharable) read lock.
  // On the other hand, operation that deals with multiple partitions or
  // modifies global variables in this class have to acquire an (exclusive) write lock.
  private final ReadWriteLock lock;

  @Inject
  private PartitionManagerMaster(final MessageEnvironment masterMessageEnvironment) {
    masterMessageEnvironment.setupListener(MessageEnvironment.PARTITION_MANAGER_MASTER_MESSAGE_LISTENER_ID,
        new PartitionManagerMasterControlMessageReceiver());
    this.partitionIdToMetadata = new HashMap<>();
    this.producerTaskGroupIdToPartitionIds = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
  }

  /**
   * Initializes the states of a partition which will be produced by producer task(s).
   *
   * @param partitionId         the id of the partition to initialize.
   * @param producerTaskGroupId the id of the producer task group.
   */
  @VisibleForTesting
  public void initializeState(final String partitionId,
                              final String producerTaskGroupId) {
    final Lock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      partitionIdToMetadata.put(partitionId, new PartitionMetadata(partitionId));
      producerTaskGroupIdToPartitionIds.putIfAbsent(producerTaskGroupId, new HashSet<>());
      producerTaskGroupIdToPartitionIds.get(producerTaskGroupId).add(partitionId);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Manages the partition information when a executor is removed.
   *
   * @param executorId the id of removed executor.
   * @return the set of task groups have to be recomputed.
   */
  public Set<String> removeWorker(final String executorId) {
    final Set<String> taskGroupsToRecompute = new HashSet<>();
    LOG.warn("Worker {} is removed.", new Object[]{executorId});

    final Lock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      // Set committed partition states to lost
      getCommittedPartitionsByWorker(executorId).forEach(partitionId -> {
        onPartitionStateChanged(partitionId, PartitionState.State.LOST, executorId);
        // producerTaskGroupForPartition should always be non-empty.
        final Set<String> producerTaskGroupForPartition = getProducerTaskGroupIds(partitionId);
        producerTaskGroupForPartition.forEach(taskGroupsToRecompute::add);
      });

      return taskGroupsToRecompute;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Returns a {@link CompletableFuture} of partition location, which is not yet resolved in {@code SCHEDULED} state.
   * TODO #446: Control the Point of Partition Fetch in Executor.
   *
   * @param partitionId id of the specified partition.
   * @return {@link CompletableFuture} of partition location, which completes exceptionally when the partition
   *         is not {@code SCHEDULED} or {@code COMMITTED}.
   */
  CompletableFuture<String> getPartitionLocationFuture(final String partitionId) {
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      final PartitionState.State state =
          (PartitionState.State) getPartitionState(partitionId).getStateMachine().getCurrentState();
      switch (state) {
        case SCHEDULED:
        case COMMITTED:
          return partitionIdToMetadata.get(partitionId).getLocationFuture();
        case READY:
        case LOST_BEFORE_COMMIT:
        case LOST:
        case REMOVED:
          final CompletableFuture<String> future = new CompletableFuture<>();
          future.completeExceptionally(new AbsentPartitionException(partitionId, state));
          return future;
        default:
          throw new UnsupportedOperationException(state.toString());
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Gets the ids of the task groups which already produced or will produce data for a specific partition.
   *
   * @param partitionId the id of the partition.
   * @return the ids of the producer task groups.
   */
  @VisibleForTesting
  public Set<String> getProducerTaskGroupIds(final String partitionId) {
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      final Set<String> producerTaskGroupIds = new HashSet<>();
      for (Map.Entry<String, Set<String>> entry : producerTaskGroupIdToPartitionIds.entrySet()) {
        if (entry.getValue().contains(partitionId)) {
          producerTaskGroupIds.add(entry.getKey());
        }
      }

      return producerTaskGroupIds;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * To be called when a potential producer task group is scheduled.
   * To be precise, it is called when the task group is enqueued to
   * {@link edu.snu.onyx.runtime.master.scheduler.PendingTaskGroupPriorityQueue}.
   *
   * @param scheduledTaskGroupId the ID of the scheduled task group.
   */
  public void onProducerTaskGroupScheduled(final String scheduledTaskGroupId) {
    final Lock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      if (producerTaskGroupIdToPartitionIds.containsKey(scheduledTaskGroupId)) {
        producerTaskGroupIdToPartitionIds.get(scheduledTaskGroupId).forEach(partitionId -> {
          if (!partitionIdToMetadata.get(partitionId).getPartitionState()
              .getStateMachine().getCurrentState().equals(SCHEDULED)) {
            onPartitionStateChanged(partitionId, SCHEDULED, null);
          }
        });
      } // else this task group does not produce any partition
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * To be called when a potential producer task group fails.
   * Only the TaskGroups that have not yet completed (i.e. partitions not yet committed) will call this method.
   *
   * @param failedTaskGroupId the ID of the task group that failed.
   */
  public void onProducerTaskGroupFailed(final String failedTaskGroupId) {
    final Lock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      if (producerTaskGroupIdToPartitionIds.containsKey(failedTaskGroupId)) {
        LOG.info("ProducerTaskGroup {} failed for a list of partitions:", failedTaskGroupId);
        producerTaskGroupIdToPartitionIds.get(failedTaskGroupId).forEach(partitionId -> {
          final PartitionState.State state = (PartitionState.State)
              partitionIdToMetadata.get(partitionId).getPartitionState().getStateMachine().getCurrentState();
          if (state == PartitionState.State.COMMITTED) {
            LOG.info("Partition lost: {}", partitionId);
            onPartitionStateChanged(partitionId, PartitionState.State.LOST, null);
          } else {
            LOG.info("Partition lost_before_commit: {}", partitionId);
            onPartitionStateChanged(partitionId, PartitionState.State.LOST_BEFORE_COMMIT, null);
          }
        });
      } // else this task group does not produce any partition
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Gets the committed partitions by an executor.
   *
   * @param executorId the id of the executor.
   * @return the committed partitions by the executor.
   */
  @VisibleForTesting
  Set<String> getCommittedPartitionsByWorker(final String executorId) {
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      final Set<String> partitionIds = new HashSet<>();
      partitionIdToMetadata.values().forEach(partitionMetadata -> {
        final String location = partitionMetadata.getLocationFuture().getNow("NOT_COMMITTED");
        if (location.equals(executorId)) {
          partitionIds.add(partitionMetadata.getPartitionId());
        }
      });
      return partitionIds;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * @return the {@link PartitionState} of a partition.
   *
   * @param partitionId the id of the partition.
   */
  @VisibleForTesting
  PartitionState getPartitionState(final String partitionId) {
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      return partitionIdToMetadata.get(partitionId).getPartitionState();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Deals with state change of a partition.
   *
   * @param partitionId     the id of the partition.
   * @param newState        the new state of the partition.
   * @param location        the location of the partition (e.g., worker id, remote store).
   *                        {@code null} if not committed or lost.
   */
  @VisibleForTesting
  public void onPartitionStateChanged(final String partitionId,
                                      final PartitionState.State newState,
                                      @Nullable final String location) {
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      partitionIdToMetadata.get(partitionId).onStateChanged(newState, location);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Deals with a request for the location of a partition.
   *
   * @param message        the request message.
   * @param messageContext the message context which will be used for response.
   */
  void onRequestPartitionLocation(final ControlMessage.Message message,
                                  final MessageContext messageContext) {
    assert (message.getType() == ControlMessage.MessageType.RequestPartitionLocation);
    final ControlMessage.RequestPartitionLocationMsg requestPartitionLocationMsg =
        message.getRequestPartitionLocationMsg();
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      final CompletableFuture<String> locationFuture
          = getPartitionLocationFuture(requestPartitionLocationMsg.getPartitionId());
      locationFuture.whenComplete((location, throwable) -> {
        final ControlMessage.PartitionLocationInfoMsg.Builder infoMsgBuilder =
            ControlMessage.PartitionLocationInfoMsg.newBuilder()
                .setRequestId(message.getId())
                .setPartitionId(requestPartitionLocationMsg.getPartitionId());
        if (throwable == null) {
          infoMsgBuilder.setOwnerExecutorId(location);
        } else {
          infoMsgBuilder.setState(
              convertPartitionState(((AbsentPartitionException) throwable).getState()));
        }
        messageContext.reply(
            ControlMessage.Message.newBuilder()
                .setId(RuntimeIdGenerator.generateMessageId())
                .setListenerId(MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID)
                .setType(ControlMessage.MessageType.PartitionLocationInfo)
                .setPartitionLocationInfoMsg(infoMsgBuilder.build())
                .build());
      });
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Reserves the region for a block in a partition, appends the block metadata,
   * and replies with the starting point of the block in the file.
   *
   * @param message        the message having the block metadata to append.
   * @param messageContext the context which will be used for response.
   */
  @VisibleForTesting
  public void onReserveBlock(final ControlMessage.Message message,
                             final MessageContext messageContext) {
    assert (message.getType() == ControlMessage.MessageType.ReserveBlock);
    final ControlMessage.ReserveBlockMsg reserveBlockMsg = message.getReserveBlockMsg();
    final String partitionId = reserveBlockMsg.getPartitionId();
    final ControlMessage.ReserveBlockResponseMsg.Builder responseBuilder =
        ControlMessage.ReserveBlockResponseMsg.newBuilder()
            .setRequestId(message.getId());

    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      final PartitionMetadata metadata = partitionIdToMetadata.get(partitionId);

      // Reserve a region for this block and append the metadata.
      final Pair<Integer, Long> reserveResult = metadata.reserveBlock(reserveBlockMsg.getBlockMetadata());
      final int blockIndex = reserveResult.left();
      final long positionToWrite = reserveResult.right();
      responseBuilder.setBlockIdx(blockIndex);
      responseBuilder.setPositionToWrite(positionToWrite);

      // Reply with the position to write in the file.
      messageContext.reply(
          ControlMessage.Message.newBuilder()
              .setId(RuntimeIdGenerator.generateMessageId())
              .setListenerId(MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID)
              .setType(ControlMessage.MessageType.ReserveBlockResponse)
              .setReserveBlockResponseMsg(responseBuilder.build())
              .build());
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Commits the blocks for a remote partition.
   *
   * @param message the message having metadata to commit.
   */
  @VisibleForTesting
  public void onCommitBlocks(final ControlMessage.Message message) {
    assert (message.getType() == ControlMessage.MessageType.CommitBlock);
    final ControlMessage.CommitBlockMsg commitMsg = message.getCommitBlockMsg();
    final String partitionId = commitMsg.getPartitionId();
    final List<Integer> blockIndices = commitMsg.getBlockIdxList();

    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      final PartitionMetadata metadata = partitionIdToMetadata.get(partitionId);
      if (metadata != null) {
        metadata.commitBlocks(blockIndices);
      } else {
        LOG.error("Metadata for {} already exists. It will be replaced.", partitionId);
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Accepts a request for the block metadata and replies with the metadata for a remote partition.
   *
   * @param message        the message having metadata to store.
   * @param messageContext the context to reply.
   */
  @VisibleForTesting
  public void onRequestBlockMetadata(final ControlMessage.Message message,
                                     final MessageContext messageContext) {
    assert (message.getType() == ControlMessage.MessageType.RequestBlockMetadata);
    final ControlMessage.RequestBlockMetadataMsg requestMsg = message.getRequestBlockMetadataMsg();
    final String partitionId = requestMsg.getPartitionId();

    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      // Check whether the partition is committed. The actual location is not important.
      final CompletableFuture<String> locationFuture = getPartitionLocationFuture(partitionId);

      locationFuture.whenComplete((location, throwable) -> {
        final ControlMessage.MetadataResponseMsg.Builder responseBuilder =
            ControlMessage.MetadataResponseMsg.newBuilder()
                .setRequestId(message.getId());
        if (throwable == null) {
          // Well committed.
          final PartitionMetadata metadata = partitionIdToMetadata.get(partitionId);
          if (metadata != null) {
            metadata.getBlockMetadataList().forEach(blockMetadataInServer ->
                responseBuilder.addBlockMetadata(blockMetadataInServer.getBlockMetadataMsg()));
          } else {
            LOG.error("Metadata for {} dose not exist. Failed to get it.", partitionId);
          }
        } else {
          responseBuilder.setState(
              convertPartitionState(((AbsentPartitionException) throwable).getState()));
        }
        messageContext.reply(
            ControlMessage.Message.newBuilder()
                .setId(RuntimeIdGenerator.generateMessageId())
                .setListenerId(MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID)
                .setType(ControlMessage.MessageType.MetadataResponse)
                .setMetadataResponseMsg(responseBuilder.build())
                .build());
      });
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Removes the block metadata for a remote partition.
   * If the target partition was not previously created, ignores this message.
   *
   * @param message the message pointing the metadata to remove.
   */
  @VisibleForTesting
  public void onRemoveBlockMetadata(final ControlMessage.Message message) {
    assert (message.getType() == ControlMessage.MessageType.RemoveBlockMetadata);
    final ControlMessage.RemoveBlockMetadataMsg removeMsg = message.getRemoveBlockMetadataMsg();
    final String partitionId = removeMsg.getPartitionId();

    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      final PartitionMetadata metadata = partitionIdToMetadata.get(partitionId);
      if (metadata != null) {
        metadata.removeBlockMetadata();
      } // if else, the partition was not previously created. Ignore it.
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Handler for control messages received.
   */
  public final class PartitionManagerMasterControlMessageReceiver implements MessageListener<ControlMessage.Message> {

    @Override
    public void onMessage(final ControlMessage.Message message) {
      try {
        switch (message.getType()) {
          case PartitionStateChanged:
            final ControlMessage.PartitionStateChangedMsg partitionStateChangedMsg =
                message.getPartitionStateChangedMsg();
            final String partitionId = partitionStateChangedMsg.getPartitionId();
            onPartitionStateChanged(partitionId, convertPartitionState(partitionStateChangedMsg.getState()),
                partitionStateChangedMsg.getLocation());
            break;
          case CommitBlock:
            onCommitBlocks(message);
            break;
          case RemoveBlockMetadata:
            onRemoveBlockMetadata(message);
            break;
          default:
            throw new IllegalMessageException(
                new Exception("This message should not be received by "
                    + PartitionManagerMaster.class.getName() + ":" + message.getType()));
        }
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void onMessageWithContext(final ControlMessage.Message message, final MessageContext messageContext) {
      switch (message.getType()) {
        case RequestPartitionLocation:
          onRequestPartitionLocation(message, messageContext);
          break;
        case RequestBlockMetadata:
          onRequestBlockMetadata(message, messageContext);
          break;
        case ReserveBlock:
          onReserveBlock(message, messageContext);
          break;
        default:
          throw new IllegalMessageException(
              new Exception("This message should not be received by "
                  + PartitionManagerMaster.class.getName() + ":" + message.getType()));
      }
    }
  }
}
