package org.apache.nemo.runtime.master;

import org.apache.nemo.common.exception.IllegalMessageException;
import org.apache.nemo.common.exception.UnknownExecutionStateException;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.exception.AbsentBlockException;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.message.MessageContext;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageListener;
import org.apache.nemo.runtime.common.state.BlockState;

import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CancellationException;
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
 * This implementation assumes that only a single user application can submit (maybe multiple) plans through
 * {@link org.apache.nemo.runtime.master.scheduler.Scheduler}.
 */
@ThreadSafe
@DriverSide
public final class BlockManagerMaster {
  private static final Logger LOG = LoggerFactory.getLogger(BlockManagerMaster.class.getName());

  private final Map<String, Set<String>> producerTaskIdToBlockIds; // a task can have multiple out-edges

  /**
   * See {@link RuntimeIdManager#generateBlockIdWildcard(String, int)} for information on block wildcards.
   */
  private final Map<String, Set<BlockMetadata>> blockIdWildcardToMetadataSet; // a metadata = a task attempt output

  // A lock that can be acquired exclusively or not.
  // Because the BlockMetadata itself is sufficiently synchronized,
  // operation that runs in a single block can just acquire a (sharable) read lock.
  // On the other hand, operation that deals with multiple blocks or
  // modifies global variables in this class have to acquire an (exclusive) write lock.
  private final ReadWriteLock lock;

  private final Random random = new Random();

  /**
   * Constructor.
   * @param masterMessageEnvironment the message environment.
   */
  @Inject
  private BlockManagerMaster(final MessageEnvironment masterMessageEnvironment) {
    masterMessageEnvironment.setupListener(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID,
      new BlockManagerMasterControlMessageReceiver());
    this.blockIdWildcardToMetadataSet = new HashMap<>();
    this.producerTaskIdToBlockIds = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
  }

  /**
   * Initializes the states of a block which will be produced by a producer task.
   * This method is idempotent thanks to the 'Set' data structures.
   * See BatchScheduler#doSchedule for details on scheduling same task attempts multiple times.
   *
   * @param blockId        the id of the block to initialize.
   * @param producerTaskId the id of the producer task.
   */
  @VisibleForTesting
  private void initializeState(final String blockId, final String producerTaskId) {
    final Lock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      // task - to - blockIds
      producerTaskIdToBlockIds.putIfAbsent(producerTaskId, new HashSet<>());
      producerTaskIdToBlockIds.get(producerTaskId).add(blockId);

      // wildcard - to - metadata
      final String wildCard = RuntimeIdManager.getWildCardFromBlockId(blockId);
      blockIdWildcardToMetadataSet.putIfAbsent(wildCard, new HashSet<>());
      blockIdWildcardToMetadataSet.get(wildCard).add(new BlockMetadata(blockId));
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
   * Get handlers of blocks that are in a particular state.
   * @param blockIdOrWildcard to query
   * @param state of the block
   * @return the handlers, empty if none matches.
   */
  public List<BlockRequestHandler> getBlockHandlers(final String blockIdOrWildcard,
                                                    final BlockState.State state) {
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      final Set<BlockMetadata> metadataSet =
        getBlockWildcardStateSet(RuntimeIdManager.getWildCardFromBlockId(blockIdOrWildcard));
      return metadataSet.stream()
        .filter(metadata -> metadata.getBlockState().equals(state))
        .map(BlockMetadata::getLocationHandler)
        .collect(Collectors.toList());
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
  public Set<String> getProducerTaskIds(final String blockId) {
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      final Set<String> producerTaskIds = new HashSet<>();
      for (Map.Entry<String, Set<String>> entry : producerTaskIdToBlockIds.entrySet()) {
        if (entry.getValue().contains(blockId)) {
          producerTaskIds.add(entry.getKey());
        }
      }

      return producerTaskIds;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * To be called when a potential producer task is scheduled.
   *
   * @param taskId   the ID of the scheduled task.
   * @param blockIds this task will produce
   */
  public void onProducerTaskScheduled(final String taskId, final Set<String> blockIds) {
    final Lock writeLock = lock.writeLock();
    writeLock.lock();
    try {
      blockIds.forEach(blockId -> initializeState(blockId, taskId));
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
      if (producerTaskIdToBlockIds.containsKey(failedTaskId)) {
        producerTaskIdToBlockIds.get(failedTaskId).forEach(blockId ->
          onBlockStateChanged(blockId, BlockState.State.NOT_AVAILABLE, null)
        );
      } // else this task has not produced any block
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
          } catch (final CancellationException | ExecutionException e) {
            // Don't add (NOT_AVAILABLE)
          } catch (final InterruptedException e) {
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
   * @return set of block metadata for the wildcard, empty if none exists.
   */
  private Set<BlockMetadata> getBlockWildcardStateSet(final String blockIdWildcard) {
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      return blockIdWildcardToMetadataSet.getOrDefault(blockIdWildcard, new HashSet<>(0));
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
      throw new RuntimeException("BlockId " + blockId + ": " + candidates.toString()); // should match only 1
    }
    return candidates.get(0);
  }

  /**
   * @param message        the request message.
   * @param messageContext the message context which will be used for response.
   */
  private void registerLocationRequest(final ControlMessage.Message message, final MessageContext messageContext) {
    assert (message.getType() == ControlMessage.MessageType.RequestBlockLocation);
    final String blockIdWildcard = message.getRequestBlockLocationMsg().getBlockIdWildcard();
    final long requestId = message.getId();
    final Lock readLock = lock.readLock();
    readLock.lock();
    try {
      // (CASE 1) Check AVAILABLE blocks.
      final List<BlockRequestHandler> availableBlocks = getBlockHandlers(blockIdWildcard, BlockState.State.AVAILABLE);
      if (!availableBlocks.isEmpty()) {
        // random pick
        // TODO #201: Let Executors Try Multiple Input Block Clones
        availableBlocks.get(random.nextInt(availableBlocks.size())).registerRequest(requestId, messageContext);
        return;
      }

      // (CASE 2) Check IN_PROGRESS blocks.
      final List<BlockRequestHandler> progressBlocks = getBlockHandlers(blockIdWildcard, BlockState.State.IN_PROGRESS);
      if (!progressBlocks.isEmpty()) {
        // random pick
        progressBlocks.get(random.nextInt(progressBlocks.size())).registerRequest(requestId, messageContext);
        return;
      }

      // (CASE 3) Unfortunately, there is no good block to use.
      final BlockRequestHandler absent = new BlockRequestHandler(blockIdWildcard);
      absent.completeExceptionally(new AbsentBlockException(blockIdWildcard, BlockState.State.NOT_AVAILABLE));
      absent.registerRequest(requestId, messageContext);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Handler for control messages received.
   */
  public final class BlockManagerMasterControlMessageReceiver implements MessageListener<ControlMessage.Message> {

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
          registerLocationRequest(message, messageContext);
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
  public static final class BlockRequestHandler {
    private final String blockIdOrWildcard;
    private final CompletableFuture<String> locationFuture;

    /**
     * Constructor.
     *
     * @param blockIdOrWildcard the ID of the block.
     */
    BlockRequestHandler(final String blockIdOrWildcard) {
      this.blockIdOrWildcard = blockIdOrWildcard;
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
          .setBlockId(blockIdOrWildcard);

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
    public CompletableFuture<String> getLocationFuture() {
      return locationFuture;
    }
  }

  /**
   * Return the corresponding {@link BlockState.State} for the specified {@link ControlMessage.BlockStateFromExecutor}.
   *
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
   *
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
