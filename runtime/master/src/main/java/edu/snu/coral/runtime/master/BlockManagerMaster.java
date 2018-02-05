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
package edu.snu.coral.runtime.master;

import edu.snu.coral.common.exception.IllegalMessageException;
import edu.snu.coral.runtime.common.exception.AbsentBlockException;
import edu.snu.coral.runtime.common.RuntimeIdGenerator;
import edu.snu.coral.runtime.common.comm.ControlMessage;
import edu.snu.coral.runtime.common.message.MessageContext;
import edu.snu.coral.runtime.common.message.MessageEnvironment;
import edu.snu.coral.runtime.common.message.MessageListener;
import edu.snu.coral.runtime.common.state.BlockState;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static edu.snu.coral.runtime.common.state.BlockState.State.SCHEDULED;
import static edu.snu.coral.runtime.master.RuntimeMaster.convertBlockState;

/**
 * Master-side block manager.
 */
@ThreadSafe
@DriverSide
public final class BlockManagerMaster {
  private static final Logger LOG = LoggerFactory.getLogger(BlockManagerMaster.class.getName());
  private final Map<String, BlockMetadata> blockIdToMetadata;
  private final Map<String, Set<String>> producerTaskGroupIdToBlockIds;
  // A lock that can be acquired exclusively or not.
  // Because the BlockMetadata itself is sufficiently synchronized,
  // operation that runs in a single block can just acquire a (sharable) read lock.
  // On the other hand, operation that deals with multiple blocks or
  // modifies global variables in this class have to acquire an (exclusive) write lock.
  private final ReadWriteLock lock;

  /**
   * Constructor.
   *
   * @param masterMessageEnvironment the message environment.
   */
  @Inject
  private BlockManagerMaster(final MessageEnvironment masterMessageEnvironment) {
    masterMessageEnvironment.setupListener(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID,
        new PartitionManagerMasterControlMessageReceiver());
    this.blockIdToMetadata = new HashMap<>();
    this.producerTaskGroupIdToBlockIds = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
  }

  /**
   * Initializes the states of a block which will be produced by producer task(s).
   *
   * @param blockId             the id of the block to initialize.
   * @param producerTaskGroupId the id of the producer task group.
   */
  @VisibleForTesting
  public void initializeState(final String blockId,
                              final String producerTaskGroupId) {
    final Lock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      blockIdToMetadata.put(blockId, new BlockMetadata(blockId));
      producerTaskGroupIdToBlockIds.putIfAbsent(producerTaskGroupId, new HashSet<>());
      producerTaskGroupIdToBlockIds.get(producerTaskGroupId).add(blockId);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Manages the block information when a executor is removed.
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
      // Set committed block states to lost
      getCommittedBlocksByWorker(executorId).forEach(blockId -> {
        onBlockStateChanged(blockId, BlockState.State.LOST, executorId);
        // producerTaskGroupForPartition should always be non-empty.
        final Set<String> producerTaskGroupForPartition = getProducerTaskGroupIds(blockId);
        producerTaskGroupForPartition.forEach(taskGroupsToRecompute::add);
      });

      return taskGroupsToRecompute;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Returns a {@link CompletableFuture} of block location, which is not yet resolved in {@code SCHEDULED} state.
   *
   * @param blockId id of the specified block.
   * @return {@link CompletableFuture} of block location, which completes exceptionally when the block
   *         is not {@code SCHEDULED} or {@code COMMITTED}.
   */
  public CompletableFuture<String> getBlockLocationFuture(final String blockId) {
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      final BlockState.State state =
          (BlockState.State) getBlockState(blockId).getStateMachine().getCurrentState();
      switch (state) {
        case SCHEDULED:
        case COMMITTED:
          return blockIdToMetadata.get(blockId).getLocationFuture();
        case READY:
        case LOST_BEFORE_COMMIT:
        case LOST:
        case REMOVED:
          final CompletableFuture<String> future = new CompletableFuture<>();
          future.completeExceptionally(new AbsentBlockException(blockId, state));
          return future;
        default:
          throw new UnsupportedOperationException(state.toString());
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Gets the ids of the task groups which already produced or will produce data for a specific block.
   *
   * @param blockId the id of the block.
   * @return the ids of the producer task groups.
   */
  @VisibleForTesting
  public Set<String> getProducerTaskGroupIds(final String blockId) {
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      final Set<String> producerTaskGroupIds = new HashSet<>();
      for (Map.Entry<String, Set<String>> entry : producerTaskGroupIdToBlockIds.entrySet()) {
        if (entry.getValue().contains(blockId)) {
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
   * {@link edu.snu.coral.runtime.master.scheduler.PendingTaskGroupQueue}.
   *
   * @param scheduledTaskGroupId the ID of the scheduled task group.
   */
  public void onProducerTaskGroupScheduled(final String scheduledTaskGroupId) {
    final Lock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      if (producerTaskGroupIdToBlockIds.containsKey(scheduledTaskGroupId)) {
        producerTaskGroupIdToBlockIds.get(scheduledTaskGroupId).forEach(blockId -> {
          if (!blockIdToMetadata.get(blockId).getBlockState()
              .getStateMachine().getCurrentState().equals(SCHEDULED)) {
            onBlockStateChanged(blockId, SCHEDULED, null);
          }
        });
      } // else this task group does not produce any block
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * To be called when a potential producer task group fails.
   * Only the TaskGroups that have not yet completed (i.e. blocks not yet committed) will call this method.
   *
   * @param failedTaskGroupId the ID of the task group that failed.
   */
  public void onProducerTaskGroupFailed(final String failedTaskGroupId) {
    final Lock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      if (producerTaskGroupIdToBlockIds.containsKey(failedTaskGroupId)) {
        LOG.info("ProducerTaskGroup {} failed for a list of blocks:", failedTaskGroupId);
        producerTaskGroupIdToBlockIds.get(failedTaskGroupId).forEach(blockId -> {
          final BlockState.State state = (BlockState.State)
              blockIdToMetadata.get(blockId).getBlockState().getStateMachine().getCurrentState();
          if (state == BlockState.State.COMMITTED) {
            LOG.info("Partition lost: {}", blockId);
            onBlockStateChanged(blockId, BlockState.State.LOST, null);
          } else {
            LOG.info("Partition lost_before_commit: {}", blockId);
            onBlockStateChanged(blockId, BlockState.State.LOST_BEFORE_COMMIT, null);
          }
        });
      } // else this task group does not produce any block
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Gets the committed blocks by an executor.
   *
   * @param executorId the id of the executor.
   * @return the committed blocks by the executor.
   */
  @VisibleForTesting
  Set<String> getCommittedBlocksByWorker(final String executorId) {
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      final Set<String> blockIds = new HashSet<>();
      blockIdToMetadata.values().forEach(blockMetadata -> {
        final String location = blockMetadata.getLocationFuture().getNow("NOT_COMMITTED");
        if (location.equals(executorId)) {
          blockIds.add(blockMetadata.getBlockId());
        }
      });
      return blockIds;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * @return the {@link BlockState} of a block.
   *
   * @param blockId the id of the block.
   */
  @VisibleForTesting
  BlockState getBlockState(final String blockId) {
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      return blockIdToMetadata.get(blockId).getBlockState();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Deals with state change of a block.
   *
   * @param blockId     the id of the block.
   * @param newState        the new state of the block.
   * @param location        the location of the block (e.g., worker id, remote store).
   *                        {@code null} if not committed or lost.
   */
  @VisibleForTesting
  public void onBlockStateChanged(final String blockId,
                                  final BlockState.State newState,
                                  @Nullable final String location) {
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      blockIdToMetadata.get(blockId).onStateChanged(newState, location);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Deals with a request for the location of a block.
   *
   * @param message        the request message.
   * @param messageContext the message context which will be used for response.
   */
  void onRequestBlockLocation(final ControlMessage.Message message,
                              final MessageContext messageContext) {
    assert (message.getType() == ControlMessage.MessageType.RequestBlockLocation);
    final String blockId = message.getRequestBlockLocationMsg().getBlockId();
    final long requestId = message.getId();
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      final CompletableFuture<String> locationFuture
          = getBlockLocationFuture(blockId);
      locationFuture.whenComplete((location, throwable) -> {
        final ControlMessage.BlockLocationInfoMsg.Builder infoMsgBuilder =
            ControlMessage.BlockLocationInfoMsg.newBuilder()
                .setRequestId(requestId)
                .setBlockId(blockId);
        if (throwable == null) {
          infoMsgBuilder.setOwnerExecutorId(location);
        } else {
          infoMsgBuilder.setState(
              convertBlockState(((AbsentBlockException) throwable).getState()));
        }
        messageContext.reply(
            ControlMessage.Message.newBuilder()
                .setId(RuntimeIdGenerator.generateMessageId())
                .setListenerId(MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID)
                .setType(ControlMessage.MessageType.BlockLocationInfo)
                .setBlockLocationInfoMsg(infoMsgBuilder.build())
                .build());
      });
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
          case BlockStateChanged:
            final ControlMessage.BlockStateChangedMsg blockStateChangedMsg =
                message.getBlockStateChangedMsg();
            final String blockId = blockStateChangedMsg.getBlockId();
            onBlockStateChanged(blockId, RuntimeMaster.convertBlockState(blockStateChangedMsg.getState()),
                blockStateChangedMsg.getLocation());
            break;
          default:
            throw new IllegalMessageException(
                new Exception("This message should not be received by "
                    + BlockManagerMaster.class.getName() + ":" + message.getType()));
        }
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void onMessageWithContext(final ControlMessage.Message message, final MessageContext messageContext) {
      switch (message.getType()) {
        case RequestBlockLocation:
          onRequestBlockLocation(message, messageContext);
          break;
        default:
          throw new IllegalMessageException(
              new Exception("This message should not be received by "
                  + BlockManagerMaster.class.getName() + ":" + message.getType()));
      }
    }
  }
}
