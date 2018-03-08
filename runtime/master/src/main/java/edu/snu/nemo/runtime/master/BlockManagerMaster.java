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
package edu.snu.nemo.runtime.master;

import edu.snu.nemo.common.exception.IllegalMessageException;
import edu.snu.nemo.common.exception.UnknownExecutionStateException;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.exception.AbsentBlockException;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.message.MessageContext;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.MessageListener;
import edu.snu.nemo.runtime.common.state.BlockState;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static edu.snu.nemo.runtime.common.state.BlockState.State.SCHEDULED;

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
   * Returns a handler of block location requests.
   *
   * @param blockId id of the specified block.
   * @return the handler of block location requests, which completes exceptionally when the block
   * is not {@code SCHEDULED} or {@code COMMITTED}.
   */
  public BlockLocationRequestHandler getBlockLocationHandler(final String blockId) {
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      final BlockState.State state =
          (BlockState.State) getBlockState(blockId).getStateMachine().getCurrentState();
      switch (state) {
        case SCHEDULED:
        case COMMITTED:
          return blockIdToMetadata.get(blockId).getLocationHandler();
        case READY:
        case LOST_BEFORE_COMMIT:
        case LOST:
        case REMOVED:
          final BlockLocationRequestHandler handler = new BlockLocationRequestHandler(blockId);
          handler.completeExceptionally(new AbsentBlockException(blockId, state));
          return handler;
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
   * {@link edu.snu.nemo.runtime.master.scheduler.PendingTaskGroupQueue}.
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
        final Future<String> location = blockMetadata.getLocationHandler().getLocationFuture();
        if (location.isDone()) {
          try {
            if (location.get().equals(executorId)) {
              blockIds.add(blockMetadata.getBlockId());
            }
          } catch (final InterruptedException | ExecutionException e) {
            // Cannot reach here because we check the completion of the future already.
            LOG.error("Exception while getting the location of a block!", e);
          }
        }
      });
      return blockIds;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * @param blockId the id of the block.
   * @return the {@link BlockState} of a block.
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
   * @param blockId  the id of the block.
   * @param newState the new state of the block.
   * @param location the location of the block (e.g., worker id, remote store).
   *                 {@code null} if not committed or lost.
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
      final BlockLocationRequestHandler locationFuture = getBlockLocationHandler(blockId);
      locationFuture.registerRequest(requestId, messageContext);
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
            onBlockStateChanged(blockId, convertBlockState(blockStateChangedMsg.getState()),
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

  /**
   * The handler of block location requests.
   */
  @VisibleForTesting
  public static final class BlockLocationRequestHandler {
    private final String blockId;
    private final CompletableFuture<String> locationFuture;

    /**
     * Constructor.
     *
     * @param blockId the ID of the block.
     */
    BlockLocationRequestHandler(final String blockId) {
      this.blockId = blockId;
      this.locationFuture = new CompletableFuture<>();
    }

    /**
     * Completes the block location future.
     * If there is any pending request, replies with the completed location.
     *
     * @param location the location of the block.
     */
    void complete(final String location) {
      locationFuture.complete(location);
    }

    /**
     * Completes the block location future with failure.
     * If there is any pending request, replies with the cause.
     *
     * @param throwable the cause of failure.
     */
    void completeExceptionally(final Throwable throwable) {
      locationFuture.completeExceptionally(throwable);
    }

    /**
     * Registers a request for the block location.
     * If the location is already known, reply the location instantly.
     *
     * @param requestId      the ID of the block location request.
     * @param messageContext the message context to reply.
     */
    void registerRequest(final long requestId,
                         final MessageContext messageContext) {
      final ControlMessage.BlockLocationInfoMsg.Builder infoMsgBuilder =
          ControlMessage.BlockLocationInfoMsg.newBuilder()
              .setRequestId(requestId)
              .setBlockId(blockId);

      locationFuture.whenComplete((location, throwable) -> {
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
    }

    /**
     * @return the future of the block location.
     */
    @VisibleForTesting
    public Future<String> getLocationFuture() {
      return locationFuture;
    }
  }

  /**
   * Return the corresponding {@link BlockState.State} for the specified {@link ControlMessage.BlockStateFromExecutor}.
   * @param state {@link ControlMessage.BlockStateFromExecutor}
   * @return the corresponding {@link BlockState.State}
   */
  public static BlockState.State convertBlockState(final ControlMessage.BlockStateFromExecutor state) {
    switch (state) {
      case BLOCK_READY:
        return BlockState.State.READY;
      case SCHEDULED:
        return BlockState.State.SCHEDULED;
      case COMMITTED:
        return BlockState.State.COMMITTED;
      case LOST_BEFORE_COMMIT:
        return BlockState.State.LOST_BEFORE_COMMIT;
      case LOST:
        return BlockState.State.LOST;
      case REMOVED:
        return BlockState.State.REMOVED;
      default:
        throw new UnknownExecutionStateException(new Exception("This BlockState is unknown: " + state));
    }
  }

  /**
   * Return the corresponding {@link ControlMessage.BlockStateFromExecutor} for the specified {@link BlockState.State}.
   * @param state {@link BlockState.State}
   * @return the corresponding {@link ControlMessage.BlockStateFromExecutor}
   */
  public static ControlMessage.BlockStateFromExecutor convertBlockState(final BlockState.State state) {
    switch (state) {
      case READY:
        return ControlMessage.BlockStateFromExecutor.BLOCK_READY;
      case SCHEDULED:
        return ControlMessage.BlockStateFromExecutor.SCHEDULED;
      case COMMITTED:
        return ControlMessage.BlockStateFromExecutor.COMMITTED;
      case LOST_BEFORE_COMMIT:
        return ControlMessage.BlockStateFromExecutor.LOST_BEFORE_COMMIT;
      case LOST:
        return ControlMessage.BlockStateFromExecutor.LOST;
      case REMOVED:
        return ControlMessage.BlockStateFromExecutor.REMOVED;
      default:
        throw new UnknownExecutionStateException(new Exception("This BlockState is unknown: " + state));
    }
  }

}
