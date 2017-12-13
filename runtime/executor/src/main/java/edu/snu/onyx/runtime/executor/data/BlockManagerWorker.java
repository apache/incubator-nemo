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
package edu.snu.onyx.runtime.executor.data;

import edu.snu.onyx.common.exception.BlockFetchException;
import edu.snu.onyx.common.exception.BlockWriteException;
import edu.snu.onyx.common.exception.UnsupportedExecutionPropertyException;
import edu.snu.onyx.common.ir.edge.executionproperty.DataStoreProperty;
import edu.snu.onyx.common.ir.edge.executionproperty.UsedDataHandlingProperty;
import edu.snu.onyx.conf.JobConf;
import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.runtime.common.data.HashRange;
import edu.snu.onyx.runtime.executor.data.blocktransfer.BlockTransfer;
import edu.snu.onyx.runtime.executor.data.stores.BlockStore;
import edu.snu.onyx.runtime.common.RuntimeIdGenerator;
import edu.snu.onyx.runtime.common.comm.ControlMessage;
import edu.snu.onyx.runtime.common.message.MessageEnvironment;
import edu.snu.onyx.common.exception.UnsupportedBlockStoreException;
import edu.snu.onyx.runtime.common.message.PersistentConnectionToMasterMap;
import edu.snu.onyx.runtime.executor.data.blocktransfer.BlockInputStream;
import edu.snu.onyx.runtime.executor.data.blocktransfer.BlockOutputStream;
import edu.snu.onyx.runtime.executor.data.stores.*;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor-side block manager.
 */
@ThreadSafe
public final class BlockManagerWorker {
  private static final Logger LOG = LoggerFactory.getLogger(BlockManagerWorker.class.getName());
  private static final String REMOTE_FILE_STORE = "REMOTE_FILE_STORE";

  private final String executorId;
  private final MemoryStore memoryStore;
  private final SerializedMemoryStore serializedMemoryStore;
  private final LocalFileStore localFileStore;
  private final RemoteFileStore remoteFileStore;
  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;
  private final ConcurrentMap<String, Coder> runtimeEdgeIdToCoder;
  private final BlockTransfer blockTransfer;
  // Executor service to schedule I/O Runnable which can be done in background.
  private final ExecutorService backgroundExecutorService;
  private final Map<String, AtomicInteger> blockToRemainingRead;

  @Inject
  private BlockManagerWorker(@Parameter(JobConf.ExecutorId.class) final String executorId,
                             @Parameter(JobConf.IORequestHandleThreadsTotal.class) final int numThreads,
                             final MemoryStore memoryStore,
                             final SerializedMemoryStore serializedMemoryStore,
                             final LocalFileStore localFileStore,
                             final RemoteFileStore remoteFileStore,
                             final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                             final BlockTransfer blockTransfer) {
    this.executorId = executorId;
    this.memoryStore = memoryStore;
    this.serializedMemoryStore = serializedMemoryStore;
    this.localFileStore = localFileStore;
    this.remoteFileStore = remoteFileStore;
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.runtimeEdgeIdToCoder = new ConcurrentHashMap<>();
    this.blockTransfer = blockTransfer;
    //this.backgroundExecutorService = Executors.newFixedThreadPool(numThreads);
    this.backgroundExecutorService = Executors.newCachedThreadPool();
    this.blockToRemainingRead = new ConcurrentHashMap<>();
  }

  /**
   * Return the coder for the specified runtime edge.
   *
   * @param runtimeEdgeId id of the runtime edge.
   * @return the corresponding coder.
   */
  public Coder getCoder(final String runtimeEdgeId) {
    final Coder coder = runtimeEdgeIdToCoder.get(runtimeEdgeId);
    if (coder == null) {
      throw new RuntimeException("No coder is registered for " + runtimeEdgeId);
    }
    return coder;
  }

  /**
   * Register a coder for runtime edge.
   *
   * @param runtimeEdgeId id of the runtime edge.
   * @param coder         the corresponding coder.
   */
  public void registerCoder(final String runtimeEdgeId, final Coder coder) {
    runtimeEdgeIdToCoder.putIfAbsent(runtimeEdgeId, coder);
  }

  /**
   * Creates a new block.
   * A stale data created by previous failed task should be handled during the creation of new block.
   *
   * @param blockId    the ID of the block to create.
   * @param blockStore the store to place the block.
   */
  public void createBlock(final String blockId,
                          final DataStoreProperty.Value blockStore) {
    final BlockStore store = getBlockStore(blockStore);
    store.createBlock(blockId);
  }

  /**
   * Retrieves data from the stored block. A specific hash value range can be designated.
   * This can be invoked multiple times per blockId (maybe due to failures).
   * Here, we first check if we have the block here, and then try to fetch the block from a remote worker.
   *
   * @param blockId       of the block.
   * @param runtimeEdgeId id of the runtime edge that corresponds to the block.
   * @param blockStore    for the data storage.
   * @param hashRange     the hash range descriptor.
   * @return the result data in the block.
   */
  public CompletableFuture<Iterable> retrieveDataFromBlock(
      final String blockId,
      final String runtimeEdgeId,
      final DataStoreProperty.Value blockStore,
      final HashRange hashRange) {
    LOG.info("RetrieveDataFromBlock: {}", blockId);
    final BlockStore store = getBlockStore(blockStore);

    // First, try to fetch the block from local BlockStore.
    final Optional<Iterable<NonSerializedPartition>> optionalResultPartitions =
        store.getPartitions(blockId, hashRange);

    if (optionalResultPartitions.isPresent()) {
      handleUsedData(blockStore, blockId);

      // Block resides in this evaluator!
      try {
        return CompletableFuture.completedFuture(DataUtil.concatNonSerPartitions(optionalResultPartitions.get()));
      } catch (final IOException e) {
        throw new BlockFetchException(e);
      }
    } else if (DataStoreProperty.Value.GlusterFileStore.equals(blockStore)) {
      throw new BlockFetchException(new Throwable("Cannot find a block in remote store."));
    } else {
      // We don't have the block here...
      return requestBlockInRemoteWorker(blockId, runtimeEdgeId, blockStore, hashRange);
    }
  }

  /**
   * Requests data in a specific hash value range from a block which resides in a remote worker asynchronously.
   * If the hash value range is [0, int.max), it will retrieve the whole data from the block.
   *
   * @param blockId       of the block.
   * @param runtimeEdgeId id of the runtime edge that corresponds to the block.
   * @param blockStore    for the data storage.
   * @param hashRange     the hash range descriptor
   * @return the {@link CompletableFuture} of the block.
   */
  private CompletableFuture<Iterable> requestBlockInRemoteWorker(
      final String blockId,
      final String runtimeEdgeId,
      final DataStoreProperty.Value blockStore,
      final HashRange hashRange) {
    // Let's see if a remote worker has it
    // Ask Master for the location
    final CompletableFuture<ControlMessage.Message> responseFromMasterFuture = persistentConnectionToMasterMap
        .getMessageSender(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID).request(
            ControlMessage.Message.newBuilder()
                .setId(RuntimeIdGenerator.generateMessageId())
                .setListenerId(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID)
                .setType(ControlMessage.MessageType.RequestBlockLocation)
                .setRequestBlockLocationMsg(
                    ControlMessage.RequestBlockLocationMsg.newBuilder()
                        .setExecutorId(executorId)
                        .setBlockId(blockId)
                        .build())
                .build());
    // Using thenCompose so that fetching block data starts after getting response from master.
    return responseFromMasterFuture.thenCompose(responseFromMaster -> {
      assert (responseFromMaster.getType() == ControlMessage.MessageType.BlockLocationInfo);
      final ControlMessage.BlockLocationInfoMsg blockLocationInfoMsg =
          responseFromMaster.getBlockLocationInfoMsg();
      if (!blockLocationInfoMsg.hasOwnerExecutorId()) {
        throw new BlockFetchException(new Throwable(
            "Block " + blockId + " not found both in the local storage and the remote storage: The"
                + "block state is " + blockLocationInfoMsg.getState()));
      }
      // This is the executor id that we wanted to know
      final String remoteWorkerId = blockLocationInfoMsg.getOwnerExecutorId();
      return blockTransfer.initiatePull(remoteWorkerId, false, blockStore, blockId,
          runtimeEdgeId, hashRange).getCompleteFuture();
    });
  }

  /**
   * Store an iterable of data partitions to a block in the target {@code BlockStore}.
   * Invariant: This should not be invoked after a block is committed.
   * Invariant: This method may not support concurrent write for a single block.
   * Only one thread have to write at once.
   *
   * @param blockId            of the block.
   * @param partitions         to save to a block.
   * @param blockStore         to store the block.
   * @param commitPerPartition whether commit every partition write or not.
   * @return a {@link Optional} of the size of each written block.
   */
  public Optional<List<Long>> putPartitions(final String blockId,
                                            final Iterable<Partition> partitions,
                                            final DataStoreProperty.Value blockStore,
                                            final boolean commitPerPartition) {
    LOG.info("PutPartitions: {}", blockId);
    final BlockStore store = getBlockStore(blockStore);

    try {
      return store.putPartitions(blockId, (Iterable) partitions, commitPerPartition);
    } catch (final Exception e) {
      throw new BlockWriteException(e);
    }
  }

  /**
   * Notifies that all writes for a block is end.
   *
   * @param blockId           the ID of the block.
   * @param blockStore        the store to save the block.
   * @param partitionSizeInfo the size metric of partitions.
   * @param srcIRVertexId     the IR vertex ID of the source task.
   * @param expectedReadTotal the expected number of read for this block.
   * @param usedDataHandling  how to handle the used block.
   */
  public void commitBlock(final String blockId,
                          final DataStoreProperty.Value blockStore,
                          final List<Long> partitionSizeInfo,
                          final String srcIRVertexId,
                          final int expectedReadTotal,
                          final UsedDataHandlingProperty.Value usedDataHandling) {
    //LOG.info("CommitBlock: {}", blockId);
    LOG.info("CommitBlock: {}, usedDataHandling: {}", blockId, usedDataHandling.toString());
    switch (usedDataHandling) {
      case Discard:
        blockToRemainingRead.put(blockId, new AtomicInteger(expectedReadTotal));
        LOG.info("CommitBlock: adding {} to the map with expected read {}", blockId, expectedReadTotal);
        break;
      case Keep:
        // Do nothing but just keep the data.
        break;
      default:
        throw new UnsupportedExecutionPropertyException("This used data handling property is not supported.");
    }

    final BlockStore store = getBlockStore(blockStore);
    store.commitBlock(blockId);
    final ControlMessage.BlockStateChangedMsg.Builder blockStateChangedMsgBuilder =
        ControlMessage.BlockStateChangedMsg.newBuilder()
            .setExecutorId(executorId)
            .setBlockId(blockId)
            .setState(ControlMessage.BlockStateFromExecutor.COMMITTED);

    if (DataStoreProperty.Value.GlusterFileStore.equals(blockStore)) {
      blockStateChangedMsgBuilder.setLocation(REMOTE_FILE_STORE);
    } else {
      blockStateChangedMsgBuilder.setLocation(executorId);
    }

    persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID)
        .send(ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setListenerId(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.BlockStateChanged)
            .setBlockStateChangedMsg(blockStateChangedMsgBuilder.build())
            .build());

    if (!partitionSizeInfo.isEmpty()) {
      // TODO #511: Refactor metric aggregation for (general) run-rime optimization.
      persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
          .send(ControlMessage.Message.newBuilder()
              .setId(RuntimeIdGenerator.generateMessageId())
              .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
              .setType(ControlMessage.MessageType.DataSizeMetric)
              .setDataSizeMetricMsg(ControlMessage.DataSizeMetricMsg.newBuilder()
                  .setBlockId(blockId)
                  .setSrcIRVertexId(srcIRVertexId)
                  .addAllPartitionSizeInfo(partitionSizeInfo)
              )
              .build());
    }
  }

  /**
   * Remove the block from store.
   *
   * @param blockId    the ID of the block to remove.
   * @param blockStore the store which contains the block.
   */
  public void removeBlock(final String blockId,
                          final DataStoreProperty.Value blockStore) {
    LOG.info("RemoveBlock: {}", blockId);
    final BlockStore store = getBlockStore(blockStore);
    final boolean exist;
    exist = store.removeBlock(blockId);

    if (exist) {
      final ControlMessage.BlockStateChangedMsg.Builder blockStateChangedMsgBuilder =
          ControlMessage.BlockStateChangedMsg.newBuilder()
              .setExecutorId(executorId)
              .setBlockId(blockId)
              .setState(ControlMessage.BlockStateFromExecutor.REMOVED);

      if (DataStoreProperty.Value.GlusterFileStore.equals(blockStore)) {
        blockStateChangedMsgBuilder.setLocation(REMOTE_FILE_STORE);
      } else {
        blockStateChangedMsgBuilder.setLocation(executorId);
      }

      persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID)
          .send(ControlMessage.Message.newBuilder()
              .setId(RuntimeIdGenerator.generateMessageId())
              .setListenerId(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID)
              .setType(ControlMessage.MessageType.BlockStateChanged)
              .setBlockStateChangedMsg(blockStateChangedMsgBuilder)
              .build());
    } else {
      throw new BlockFetchException(new Throwable("Cannot find corresponding block " + blockId));
    }
  }

  /**
   * Handles used {@link edu.snu.onyx.runtime.executor.data.block.Block}.
   *
   * @param blockStore the store which contains the block.
   * @param blockId    the ID of the block.
   */
  private void handleUsedData(final DataStoreProperty.Value blockStore,
                              final String blockId) {
    final AtomicInteger remainingExpectedRead = blockToRemainingRead.get(blockId);
    if (remainingExpectedRead != null) {
      LOG.info("handleUsedData: remainingExpectedRead for {} is {}", blockId, remainingExpectedRead.get());
      if (remainingExpectedRead.decrementAndGet() == 0) {
        // This block should be discarded.
        blockToRemainingRead.remove(blockId);
        backgroundExecutorService.submit(new Runnable() {
          @Override
          public void run() {
            removeBlock(blockId, blockStore);
          }
        });
      }
    } // If null, just keep the data in the store.
  }

  private BlockStore getBlockStore(final DataStoreProperty.Value blockStore) {
    switch (blockStore) {
      case MemoryStore:
        return memoryStore;
      case SerializedMemoryStore:
        return serializedMemoryStore;
      case LocalFileStore:
        return localFileStore;
      case GlusterFileStore:
        return remoteFileStore;
      default:
        throw new UnsupportedBlockStoreException(new Exception(blockStore + " is not supported."));
    }
  }

  /**
   * Respond to a pull request by another executor.
   * <p>
   * This method is executed by {@link edu.snu.onyx.runtime.executor.data.blocktransfer.BlockTransport} thread. \
   * Never execute a blocking call in this method!
   *
   * @param outputStream {@link BlockOutputStream}
   */
  public void onPullRequest(final BlockOutputStream<?> outputStream) {
    // We are getting the block from local store!
    final Optional<DataStoreProperty.Value> blockStoreOptional = outputStream.getBlockStore();
    final DataStoreProperty.Value blockStore = blockStoreOptional.get();

    backgroundExecutorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          if (DataStoreProperty.Value.LocalFileStore.equals(blockStore)
              || DataStoreProperty.Value.GlusterFileStore.equals(blockStore)) {
            final FileStore fileStore = (FileStore) getBlockStore(blockStore);
            outputStream.writeFileAreas(fileStore.getFileAreas(outputStream.getBlockId(),
                outputStream.getHashRange())).close();
            handleUsedData(blockStore, outputStream.getBlockId());
          } else if (DataStoreProperty.Value.SerializedMemoryStore.equals(blockStore)) {
            final SerializedMemoryStore serMemoryStore = (SerializedMemoryStore) getBlockStore(blockStore);
            final Optional<Iterable<SerializedPartition>> optionalResult = serMemoryStore.getSerializedPartitions(
                outputStream.getBlockId(), outputStream.getHashRange());
            outputStream.writeSerializedPartitions(optionalResult.get()).close();
            handleUsedData(blockStore, outputStream.getBlockId());
          } else {
            final Iterable block =
                retrieveDataFromBlock(outputStream.getBlockId(), outputStream.getRuntimeEdgeId(),
                    blockStore, outputStream.getHashRange()).get();
            outputStream.writeElements(block).close();
          }
        } catch (final IOException | ExecutionException | InterruptedException | BlockFetchException e) {
          LOG.error("Closing a pull request exceptionally", e);
          outputStream.closeExceptionally(e);
        }
      }
    });
  }

  /**
   * Respond to a push notification by another executor.
   * <p>
   * A push notification is generated when a remote executor invokes {@link edu.snu.onyx.runtime.executor.data
   * .blocktransfer.BlockTransfer#initiatePush(String, boolean, String, String, HashRange)} to transfer
   * a block to another executor.
   * <p>
   * This method is executed by {@link edu.snu.onyx.runtime.executor.data.blocktransfer.BlockTransport}
   * thread. Never execute a blocking call in this method!
   *
   * @param inputStream {@link BlockInputStream}
   */
  public void onPushNotification(final BlockInputStream inputStream) {
  }
}
