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
package edu.snu.nemo.runtime.executor.data;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import edu.snu.nemo.common.exception.BlockFetchException;
import edu.snu.nemo.common.exception.BlockWriteException;
import edu.snu.nemo.common.exception.UnsupportedBlockStoreException;
import edu.snu.nemo.common.exception.UnsupportedExecutionPropertyException;
import edu.snu.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.UsedDataHandlingProperty;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.comm.ControlMessage.ByteTransferContextDescriptor;
import edu.snu.nemo.runtime.common.data.KeyRange;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import edu.snu.nemo.runtime.executor.bytetransfer.ByteInputContext;
import edu.snu.nemo.runtime.executor.bytetransfer.ByteOutputContext;
import edu.snu.nemo.runtime.executor.bytetransfer.ByteTransfer;
import edu.snu.nemo.runtime.executor.data.partition.NonSerializedPartition;
import edu.snu.nemo.runtime.executor.data.partition.SerializedPartition;
import edu.snu.nemo.runtime.executor.data.stores.BlockStore;
import edu.snu.nemo.runtime.executor.data.stores.*;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.IOException;
import java.io.Serializable;
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
  private final ByteTransfer byteTransfer;
  // Executor service to schedule I/O Runnable which can be done in background.
  private final ExecutorService backgroundExecutorService;
  private final Map<String, AtomicInteger> blockToRemainingRead;
  private final SerializerManager serializerManager;
  private final Map<String, CompletableFuture<ControlMessage.Message>> pendingBlockLocationRequest;

  @Inject
  private BlockManagerWorker(@Parameter(JobConf.ExecutorId.class) final String executorId,
                             @Parameter(JobConf.IORequestHandleThreadsTotal.class) final int numThreads,
                             final MemoryStore memoryStore,
                             final SerializedMemoryStore serializedMemoryStore,
                             final LocalFileStore localFileStore,
                             final RemoteFileStore remoteFileStore,
                             final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                             final ByteTransfer byteTransfer,
                             final SerializerManager serializerManager) {
    this.executorId = executorId;
    this.memoryStore = memoryStore;
    this.serializedMemoryStore = serializedMemoryStore;
    this.localFileStore = localFileStore;
    this.remoteFileStore = remoteFileStore;
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.byteTransfer = byteTransfer;
    this.backgroundExecutorService = Executors.newFixedThreadPool(numThreads);
    this.blockToRemainingRead = new ConcurrentHashMap<>();
    this.serializerManager = serializerManager;
    this.pendingBlockLocationRequest = new ConcurrentHashMap<>();
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
   *
   * @param blockId    of the block.
   * @param blockStore for the data storage.
   * @param keyRange   the key range descriptor.
   * @return the result data in the block.
   */
  private CompletableFuture<DataUtil.IteratorWithNumBytes> retrieveDataFromBlock(
      final String blockId,
      final DataStoreProperty.Value blockStore,
      final KeyRange keyRange) {
    LOG.info("RetrieveDataFromBlock: {}", blockId);
    final BlockStore store = getBlockStore(blockStore);

    // First, try to fetch the block from local BlockStore.
    final Optional<Iterable<NonSerializedPartition>> optionalResultPartitions =
        store.readPartitions(blockId, keyRange);

    if (optionalResultPartitions.isPresent()) {
      handleUsedData(blockStore, blockId);

      // Block resides in this evaluator!
      try {
        final Iterator innerIterator = DataUtil.concatNonSerPartitions(optionalResultPartitions.get()).iterator();
        long numSerializedBytes = 0;
        long numEncodedBytes = 0;
        try {
          for (final NonSerializedPartition partition : optionalResultPartitions.get()) {
            numSerializedBytes += partition.getNumSerializedBytes();
            numEncodedBytes += partition.getNumEncodedBytes();
          }
          return CompletableFuture.completedFuture(DataUtil.IteratorWithNumBytes.of(innerIterator, numSerializedBytes,
              numEncodedBytes));
        } catch (final DataUtil.IteratorWithNumBytes.NumBytesNotSupportedException e) {
          return CompletableFuture.completedFuture(DataUtil.IteratorWithNumBytes.of(innerIterator));
        }
      } catch (final IOException e) {
        throw new BlockFetchException(e);
      }
    } else {
      // We don't have the block here...
      throw new RuntimeException(String.format("Block %s not found in local BlockManagerWorker", blockId));
    }
  }

  /**
   * Inquiries the location of the specific block and routes the request to the local block manager worker
   * or to the lower data plane.
   * This can be invoked multiple times per blockId (maybe due to failures).
   *
   * @param blockId       of the block.
   * @param runtimeEdgeId id of the runtime edge that corresponds to the block.
   * @param blockStore    for the data storage.
   * @param keyRange      the key range descriptor
   * @return the {@link CompletableFuture} of the block.
   */
  public CompletableFuture<DataUtil.IteratorWithNumBytes> queryBlock(
      final String blockId,
      final String runtimeEdgeId,
      final DataStoreProperty.Value blockStore,
      final KeyRange keyRange) {
    // Let's see if a remote worker has it
    final CompletableFuture<ControlMessage.Message> blockLocationFuture =
        pendingBlockLocationRequest.computeIfAbsent(blockId, blockIdToRequest -> {
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
          return responseFromMasterFuture;
        });
    blockLocationFuture.whenComplete((message, throwable) -> pendingBlockLocationRequest.remove(blockId));

    // Using thenCompose so that fetching block data starts after getting response from master.
    return blockLocationFuture.thenCompose(responseFromMaster -> {
      if (responseFromMaster.getType() != ControlMessage.MessageType.BlockLocationInfo) {
        throw new RuntimeException("Response message type mismatch!");
      }
      final ControlMessage.BlockLocationInfoMsg blockLocationInfoMsg =
          responseFromMaster.getBlockLocationInfoMsg();
      if (!blockLocationInfoMsg.hasOwnerExecutorId()) {
        throw new BlockFetchException(new Throwable(
            "Block " + blockId + " not found both in any storage: "
                + "The block state is " + blockLocationInfoMsg.getState()));
      }
      // This is the executor id that we wanted to know
      final String targetExecutorId = blockLocationInfoMsg.getOwnerExecutorId();
      if (targetExecutorId.equals(executorId) || targetExecutorId.equals(REMOTE_FILE_STORE)) {
        // Block resides in the evaluator
        return retrieveDataFromBlock(blockId, blockStore, keyRange);
      } else {
        final ByteTransferContextDescriptor descriptor = ByteTransferContextDescriptor.newBuilder()
            .setBlockId(blockId)
            .setBlockStore(convertBlockStore(blockStore))
            .setRuntimeEdgeId(runtimeEdgeId)
            .setKeyRange(ByteString.copyFrom(SerializationUtils.serialize(keyRange)))
            .build();
        return byteTransfer.newInputContext(targetExecutorId, descriptor.toByteArray())
            .thenCompose(context -> context.getCompletedFuture())
            .thenApply(streams -> new DataUtil.InputStreamIterator(streams,
                serializerManager.getSerializer(runtimeEdgeId)));
      }
    });
  }


  /**
   * Writes an element to a block in the target {@code BlockStore}.
   * Invariant: This should not be invoked after the block is committed.
   * Invariant: This method may not support concurrent write for a single block.
   *            Only one thread have to write at once.
   *
   * @param blockId    the ID of the block to write.
   * @param key        the key of the partition in the block to write the element.
   * @param element    the element to write.
   * @param blockStore the store contains the target block.
   * @param <K>        the key type of the block to write.
   */
  public <K extends Serializable> void write(final String blockId,
                                             final K key,
                                             final Object element,
                                             final DataStoreProperty.Value blockStore) {
    final BlockStore store = getBlockStore(blockStore);
    try {
      store.write(blockId, key, element);
    } catch (final Exception e) {
      throw new BlockWriteException(e);
    }
  }

  /**
   * Notifies that all writes for a block is end.
   *
   * @param blockId              the ID of the block.
   * @param blockStore           the store to save the block.
   * @param reportPartitionSizes whether report the size of partitions to master or not.
   * @param srcIRVertexId        the IR vertex ID of the source task.
   * @param expectedReadTotal    the expected number of read for this block.
   * @param usedDataHandling     how to handle the used block.
   * @return a {@link Optional} of the size of each written block.
   */
  public Optional<Long> commitBlock(final String blockId,
                                    final DataStoreProperty.Value blockStore,
                                    final boolean reportPartitionSizes,
                                    final String srcIRVertexId,
                                    final int expectedReadTotal,
                                    final UsedDataHandlingProperty.Value usedDataHandling) {
    LOG.info("CommitBlock: {}", blockId);
    switch (usedDataHandling) {
      case Discard:
        blockToRemainingRead.put(blockId, new AtomicInteger(expectedReadTotal));
        break;
      case Keep:
        // Do nothing but just keep the data.
        break;
      default:
        throw new UnsupportedExecutionPropertyException("This used data handling property is not supported.");
    }

    final BlockStore store = getBlockStore(blockStore);
    final Optional<Map<Integer, Long>> partitionSizeMap = store.commitBlock(blockId);
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

    if (reportPartitionSizes && partitionSizeMap.isPresent()) {
      final List<ControlMessage.PartitionSizeEntry> partitionSizeEntries = new ArrayList<>();
      partitionSizeMap.get().forEach((key, size) ->
          partitionSizeEntries.add(
              ControlMessage.PartitionSizeEntry.newBuilder()
                  .setKey(key)
                  .setSize(size)
                  .build())
      );

      // TODO #4: Refactor metric aggregation for (general) run-rime optimization.
      persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
          .send(ControlMessage.Message.newBuilder()
              .setId(RuntimeIdGenerator.generateMessageId())
              .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
              .setType(ControlMessage.MessageType.DataSizeMetric)
              .setDataSizeMetricMsg(ControlMessage.DataSizeMetricMsg.newBuilder()
                  .setBlockId(blockId)
                  .setSrcIRVertexId(srcIRVertexId)
                  .addAllPartitionSize(partitionSizeEntries)
              )
              .build());
    }

    // Return the total size of the committed block.
    if (partitionSizeMap.isPresent()) {
      long blockSizeTotal = 0;
      for (final long partitionSize : partitionSizeMap.get().values()) {
        blockSizeTotal += partitionSize;
      }
      return Optional.of(blockSizeTotal);
    } else {
      return Optional.empty();
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
    final boolean exist = store.removeBlock(blockId);

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
   * Handles used {@link edu.snu.nemo.runtime.executor.data.block.Block}.
   *
   * @param blockStore the store which contains the block.
   * @param blockId    the ID of the block.
   */
  private void handleUsedData(final DataStoreProperty.Value blockStore,
                              final String blockId) {
    final AtomicInteger remainingExpectedRead = blockToRemainingRead.get(blockId);
    if (remainingExpectedRead != null) {
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

  /**
   * Gets the {@link BlockStore} from annotated value of {@link DataStoreProperty}.
   * @param blockStore the annotated value of {@link DataStoreProperty}.
   * @return the block store.
   */
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
   * Respond to a block request by another executor.
   * <p>
   * This method is executed by {edu.snu.nemo.runtime.executor.data.blocktransfer.BlockTransport} thread. \
   * Never execute a blocking call in this method!
   *
   * @param outputContext {@link ByteOutputContext}
   * @throws InvalidProtocolBufferException from errors during parsing context descriptor
   */
  public void onOutputContext(final ByteOutputContext outputContext) throws InvalidProtocolBufferException {
    final ByteTransferContextDescriptor descriptor = ByteTransferContextDescriptor.PARSER
        .parseFrom(outputContext.getContextDescriptor());
    final DataStoreProperty.Value blockStore = convertBlockStore(descriptor.getBlockStore());
    final String blockId = descriptor.getBlockId();
    final KeyRange keyRange = SerializationUtils.deserialize(descriptor.getKeyRange().toByteArray());

    backgroundExecutorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          if (DataStoreProperty.Value.LocalFileStore.equals(blockStore)
              || DataStoreProperty.Value.GlusterFileStore.equals(blockStore)) {
            final FileStore fileStore = (FileStore) getBlockStore(blockStore);
            for (final FileArea fileArea : fileStore.getFileAreas(blockId, keyRange)) {
              outputContext.newOutputStream().writeFileArea(fileArea).close();
            }
          } else {
            final Optional<Iterable<SerializedPartition>> optionalResult = getBlockStore(blockStore)
                .readSerializedPartitions(blockId, keyRange);
            for (final SerializedPartition partition : optionalResult.get()) {
              outputContext.newOutputStream().writeSerializedPartition(partition).close();
            }
          }
          handleUsedData(blockStore, blockId);
          outputContext.close();
        } catch (final IOException | BlockFetchException e) {
          LOG.error("Closing a block request exceptionally", e);
          outputContext.onChannelError(e);
        }
      }
    });
  }

  /**
   * Respond to a block notification by another executor.
   * <p>
   * This method is executed by {edu.snu.nemo.runtime.executor.data.blocktransfer.BlockTransport}
   * thread. Never execute a blocking call in this method!
   *
   * @param inputContext {@link ByteInputContext}
   */
  public void onInputContext(final ByteInputContext inputContext) {
  }

  /**
   * Decodes BlockStore property from protocol buffer.
   * @param blockStore property from protocol buffer
   * @return the corresponding {@link DataStoreProperty} value
   */
  private static ControlMessage.BlockStore convertBlockStore(
      final DataStoreProperty.Value blockStore) {
    switch (blockStore) {
      case MemoryStore:
        return ControlMessage.BlockStore.MEMORY;
      case SerializedMemoryStore:
        return ControlMessage.BlockStore.SER_MEMORY;
      case LocalFileStore:
        return ControlMessage.BlockStore.LOCAL_FILE;
      case GlusterFileStore:
        return ControlMessage.BlockStore.REMOTE_FILE;
      default:
        throw new UnsupportedBlockStoreException(new Exception(blockStore + " is not supported."));
    }
  }


  /**
   * Encodes {@link DataStoreProperty} value into protocol buffer property.
   * @param blockStoreType {@link DataStoreProperty} value
   * @return the corresponding {@link ControlMessage.BlockStore} value
   */
  private static DataStoreProperty.Value convertBlockStore(
      final ControlMessage.BlockStore blockStoreType) {
    switch (blockStoreType) {
      case MEMORY:
        return DataStoreProperty.Value.MemoryStore;
      case SER_MEMORY:
        return DataStoreProperty.Value.SerializedMemoryStore;
      case LOCAL_FILE:
        return DataStoreProperty.Value.LocalFileStore;
      case REMOTE_FILE:
        return DataStoreProperty.Value.GlusterFileStore;
      default:
        throw new UnsupportedBlockStoreException(new Exception("This block store is not yet supported"));
    }
  }
}
