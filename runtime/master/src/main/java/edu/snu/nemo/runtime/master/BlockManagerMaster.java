/*
 * Copyright (C) 2018 Seoul National University
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

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.exception.IllegalMessageException;
import edu.snu.nemo.common.exception.UnknownExecutionStateException;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.exception.AbsentBlockException;
import edu.snu.nemo.runtime.common.RuntimeIdManager;
import edu.snu.nemo.runtime.common.message.MessageContext;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.MessageListener;
import edu.snu.nemo.runtime.common.plan.PhysicalPlan;
import edu.snu.nemo.runtime.common.plan.Stage;
import edu.snu.nemo.runtime.common.plan.StageEdge;
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
import java.util.stream.Collectors;

import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Master-side block manager.
 */
@ThreadSafe
@DriverSide
public final class BlockManagerMaster {
  private static final Logger LOG = LoggerFactory.getLogger(BlockManagerMaster.class.getName());
  private final Map<String, Set<BlockMetadata>> blockIdWildcardToMetadataSet; // a metadata = a task attempt output
  private final Map<String, Set<String>> producerTaskIdToBlockIdWildcards; // a task can have multiple out-edges
  // A lock that can be acquired exclusively or not.
  // Because the BlockMetadata itself is sufficiently synchronized,
  // operation that runs in a single block can just acquire a (sharable) read lock.
  // On the other hand, operation that deals with multiple blocks or
  // modifies global variables in this class have to acquire an (exclusive) write lock.
  private final ReadWriteLock lock;

  private final Random random = new Random();

  private PhysicalPlan physicalPlan;

  /**
   * Constructor.
   *
   * @param masterMessageEnvironment the message environment.
   */
  @Inject
  private BlockManagerMaster(final MessageEnvironment masterMessageEnvironment) {
    masterMessageEnvironment.setupListener(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID,
        new PartitionManagerMasterControlMessageReceiver());
    this.blockIdWildcardToMetadataSet = new HashMap<>();
    this.producerTaskIdToBlockIdWildcards = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
  }

  void initialize(final PhysicalPlan plan) {
    this.physicalPlan = plan;
    // States are initialized lazily.
  }

  /**
   * Initializes the states of a block which will be produced by a producer task.
   *
   * @param blockId             the id of the block to initialize.
   * @param producerTaskId      the id of the producer task.
   */
  @VisibleForTesting
  public void initializeState(final String blockId,
                              final String producerTaskId) {
    final Lock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      final String wildCard = RuntimeIdManager.getWildCardFromBlockId(blockId);

      // metadata
      blockIdWildcardToMetadataSet.putIfAbsent(wildCard, new HashSet<>());
      blockIdWildcardToMetadataSet.get(wildCard).add(new BlockMetadata(blockId));

      // taskToWildcards
      producerTaskIdToBlockIdWildcards.putIfAbsent(producerTaskId, new HashSet<>());
      producerTaskIdToBlockIdWildcards.get(producerTaskId).add(wildCard);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Manages the block information when a executor is removed.
   *
   * @param executorId the id of removed executor.
   * @return the set of tasks have to be recomputed.
   */
  public Set<String> removeWorker(final String executorId) {
    final Set<String> tasksToRecompute = new HashSet<>();

    final Lock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      // Set committed block states to lost
      getCommittedBlocksByWorker(executorId).forEach(blockId -> {
        onBlockStateChanged(blockId, BlockState.State.NOT_AVAILABLE, executorId);
        // producerTaskForPartition should always be non-empty.
        final Set<String> producerTaskForPartition = getProducerTaskIds(blockId);
        producerTaskForPartition.forEach(tasksToRecompute::add);
      });

      return tasksToRecompute;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Returns a handler of block location requests.
   *
   * @param blockId id of the specified block.
   * @return the handler of block location requests, which completes exceptionally when the block
   * is not {@code IN_PROGRESS} or {@code AVAILABLE}.
   */
  public BlockRequestHandler getBlockLocationHandler(final String blockId) {
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      final Set<BlockMetadata> metadataSet = getBlockWildcardStateSet(RuntimeIdManager.getWildCardFromBlockId(blockId));

      final List<BlockMetadata> candidates = metadataSet.stream()
          .filter(state -> state.equals(BlockState.State.IN_PROGRESS) || state.equals(BlockState.State.AVAILABLE))
          .collect(Collectors.toList());

      if (!candidates.isEmpty()) {
        // Randomly pick one of the candidate handlers.
        return candidates.get(random.nextInt(candidates.size())).getLocationHandler();
      } else {
        // No candidate exists
        final BlockRequestHandler handler = new BlockRequestHandler(blockId);
        handler.completeExceptionally(new AbsentBlockException(blockId, BlockState.State.NOT_AVAILABLE));
        return handler;
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Gets the ids of the tasks which already produced or will produce data for a specific block.
   *
   * @param blockId the id of the block.
   * @return the ids of the producer tasks.
   */
  @VisibleForTesting
  public Set<String> getProducerTaskIds(final String blockId) {
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      final Set<String> producerTaskIds = new HashSet<>();
      for (Map.Entry<String, Set<String>> entry : producerTaskIdToBlockIdWildcards.entrySet()) {
        if (entry.getValue().contains(blockId)) {
          producerTaskIds.add(entry.getKey());
        }
      }

      return producerTaskIds;
    } finally {
      readLock.unlock();
    }
  }

  public Set<String> getIdsOfBlocksProducedBy(final String taskId) {
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      return producerTaskIdToBlockIdWildcards.get(taskId);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * To be called when a potential producer task is scheduled.
   * @param taskId the ID of the scheduled task.
   */
  public void onProducerTaskScheduled(final String taskId) {
    final Lock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      final DAG<Stage, StageEdge> stageDAG = physicalPlan.getStageDAG();
      final Stage stage = stageDAG.getVertexById(RuntimeIdManager.getStageIdFromTaskId(taskId));

      // Inter-stage edges only (Intra-stage edges are skipped)
      stageDAG.getOutgoingEdgesOf(stage).forEach(stageEdge -> {
        final String blockId = RuntimeIdManager.generateBlockId(stageEdge.getId(), taskId);
        initializeState(blockId, taskId);
      });


      /*
      if (producerTaskIdToBlockIdWildcards.containsKey(scheduledTaskId)) {
        producerTaskIdToBlockIdWildcards.get(scheduledTaskId).forEach(wildcard -> {
          if (blockIdToMetadata.get(wildcard).getBlockState()
              .getStateMachine().getCurrentState().equals(NOT_AVAILABLE)) {
            onBlockStateChanged(wildcard, IN_PROGRESS, null);
          }
        });
      } // else this task does not produce any block
      */
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * To be called when a potential producer task fails.
   * Only the Tasks that have not yet completed (i.e. blocks not yet committed) will call this method.
   *
   * @param failedTaskId the ID of the task that failed.
   */
  public void onProducerTaskFailed(final String failedTaskId) {
    final Lock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      if (producerTaskIdToBlockIdWildcards.containsKey(failedTaskId)) {
        producerTaskIdToBlockIdWildcards.get(failedTaskId).forEach(blockId -> {
          LOG.info("Block lost: {}", blockId);
          onBlockStateChanged(blockId, BlockState.State.NOT_AVAILABLE, null);
        });
      } // else this task does not produce any block
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
  private Set<String> getCommittedBlocksByWorker(final String executorId) {
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      final Set<String> blockIds = new HashSet<>();
      blockIdWildcardToMetadataSet.values().stream().flatMap(Set::stream).forEach(blockMetadata -> {
        final Future<String> location = blockMetadata.getLocationHandler().getLocationFuture();
        if (location.isDone()) {
          try {
            if (location.get().equals(executorId)) {
              blockIds.add(blockMetadata.getBlockId());
            }
          } catch (final InterruptedException | ExecutionException e) {
            // Cannot reach here because we check the completion of the future already.
            LOG.error("Exception while getting the location of a block!", e);
            Thread.currentThread().interrupt();
          }
        }
      });
      return blockIds;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * @param blockIdWildcard to query.
   * @return the {@link BlockState} set for a block wildcard.
   */
  @VisibleForTesting
  public Set<BlockMetadata> getBlockWildcardStateSet(final String blockIdWildcard) {
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      return blockIdWildcardToMetadataSet.get(blockIdWildcard);
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
      getBlockMetaData(blockId).onStateChanged(newState, location);
    } finally {
      readLock.unlock();
    }
  }

  private BlockMetadata getBlockMetaData(final String blockId) {
    final List<BlockMetadata> candidates =
        blockIdWildcardToMetadataSet.get(RuntimeIdManager.getWildCardFromBlockId(blockId))
            .stream()
            .filter(meta -> meta.getBlockId().equals(blockId))
            .collect(Collectors.toList());

    if (candidates.size() != 1) {
      throw new RuntimeException(candidates.toString()); // should match only 1
    }

    return candidates.get(0);
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
      final BlockRequestHandler locationFuture = getBlockLocationHandler(blockId);
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
  public static final class BlockRequestHandler {
    private final String blockId;
    private final CompletableFuture<String> locationFuture;

    /**
     * Constructor.
     *
     * @param blockId the ID of the block.
     */
    BlockRequestHandler(final String blockId) {
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
                .setId(RuntimeIdManager.generateMessageId())
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
      case NOT_AVAILABLE:
        return BlockState.State.NOT_AVAILABLE;
      case IN_PROGRESS:
        return BlockState.State.IN_PROGRESS;
      case AVAILABLE:
        return BlockState.State.AVAILABLE;
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
      case NOT_AVAILABLE:
        return ControlMessage.BlockStateFromExecutor.NOT_AVAILABLE;
      case IN_PROGRESS:
        return ControlMessage.BlockStateFromExecutor.IN_PROGRESS;
      case AVAILABLE:
        return ControlMessage.BlockStateFromExecutor.AVAILABLE;
      default:
        throw new UnknownExecutionStateException(new Exception("This BlockState is unknown: " + state));
    }
  }

}
