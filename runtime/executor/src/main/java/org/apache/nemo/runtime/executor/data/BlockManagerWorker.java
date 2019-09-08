/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.executor.data;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.KeyRange;
import org.apache.nemo.common.exception.BlockFetchException;
import org.apache.nemo.common.exception.BlockWriteException;
import org.apache.nemo.common.exception.UnsupportedBlockStoreException;
import org.apache.nemo.common.exception.UnsupportedExecutionPropertyException;
import org.apache.nemo.common.ir.edge.executionproperty.DataPersistenceProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.PersistentConnectionToMasterMap;
import org.apache.nemo.runtime.executor.bytetransfer.ByteInputContext;
import org.apache.nemo.runtime.executor.bytetransfer.ByteOutputContext;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransfer;
import org.apache.nemo.runtime.executor.data.block.Block;
import org.apache.nemo.runtime.executor.data.block.FileBlock;
import org.apache.nemo.runtime.executor.data.block.SerializedMemoryBlock;
import org.apache.nemo.runtime.executor.data.partition.NonSerializedPartition;
import org.apache.nemo.runtime.executor.data.partition.SerializedPartition;
import org.apache.nemo.runtime.executor.data.stores.*;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Executor-side block manager.
 */
@ThreadSafe
public final class BlockManagerWorker {
  private static final Logger LOG = LoggerFactory.getLogger(BlockManagerWorker.class.getName());
  private static final String REMOTE_FILE_STORE = "REMOTE_FILE_STORE";

  private final String executorId;
  private final SerializerManager serializerManager;

  // Block stores
  private final MemoryStore memoryStore;
  private final SerializedMemoryStore serializedMemoryStore;
  private final LocalFileStore localFileStore;
  private final RemoteFileStore remoteFileStore;

  // To-Master connections
  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;
  private final Map<String, CompletableFuture<ControlMessage.Message>> pendingBlockLocationRequest;

  // To-Executor connections
  private final ByteTransfer byteTransfer;
  private final ExecutorService backgroundExecutorService;
  private final Map<String, AtomicInteger> blockToRemainingRead;
  private final BlockTransferThrottler blockTransferThrottler;

  /**
   * Constructor.
   *
   * @param executorId                      the executor ID.
   * @param numThreads                      the number of threads to be used for background IO request handling.
   * @param memoryStore                     the memory store.
   * @param serializedMemoryStore           the serialized memory store.
   * @param localFileStore                  the local file store.
   * @param remoteFileStore                 the remote file store.
   * @param persistentConnectionToMasterMap the connection map.
   * @param byteTransfer                    the byte transfer.
   * @param serializerManager               the serializer manager.
   * @param blockTransferThrottler          restricts parallel connections
   */
  @Inject
  private BlockManagerWorker(@Parameter(JobConf.ExecutorId.class) final String executorId,
                             @Parameter(JobConf.IORequestHandleThreadsTotal.class) final int numThreads,
                             final MemoryStore memoryStore,
                             final SerializedMemoryStore serializedMemoryStore,
                             final LocalFileStore localFileStore,
                             final RemoteFileStore remoteFileStore,
                             final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                             final ByteTransfer byteTransfer,
                             final SerializerManager serializerManager,
                             final BlockTransferThrottler blockTransferThrottler) {
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
    this.blockTransferThrottler = blockTransferThrottler;
  }

  //////////////////////////////////////////////////////////// Main public methods

  /**
   * Creates a new block.
   *
   * @param blockId    the ID of the block to create.
   * @param blockStore the store to place the block.
   * @return the created block.
   * @throws BlockWriteException for any error occurred while trying to create a block.
   */
  public Block createBlock(final String blockId,
                           final DataStoreProperty.Value blockStore) throws BlockWriteException {
    final BlockStore store = getBlockStore(blockStore);
    return store.createBlock(blockId);
  }

  /**
   * Inquiries the location of the specific block and routes the request to the local block manager worker
   * or to the lower data plane.
   * This can be invoked multiple times per blockId (maybe due to failures).
   *
   * @param blockIdWildcard of the block.
   * @param runtimeEdgeId   id of the runtime edge that corresponds to the block.
   * @param blockStore      for the data storage.
   * @param keyRange        the key range descriptor
   * @return the {@link CompletableFuture} of the block.
   */
  public CompletableFuture<DataUtil.IteratorWithNumBytes> readBlock(
    final String blockIdWildcard,
    final String runtimeEdgeId,
    final DataStoreProperty.Value blockStore,
    final KeyRange keyRange) {
    // Let's see if a remote worker has it
    final CompletableFuture<ControlMessage.Message> blockLocationFuture =
      pendingBlockLocationRequest.computeIfAbsent(blockIdWildcard, blockIdToRequest -> {
        // Ask Master for the location.
        // (IMPORTANT): This 'request' effectively blocks the TaskExecutor thread if the block is IN_PROGRESS.
        // We use this property to make the receiver task of a 'push' edge to wait in an Executor for its input data
        // to become available.
        final CompletableFuture<ControlMessage.Message> responseFromMasterFuture = persistentConnectionToMasterMap
          .getMessageSender(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID).request(
            ControlMessage.Message.newBuilder()
              .setId(RuntimeIdManager.generateMessageId())
              .setListenerId(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID)
              .setType(ControlMessage.MessageType.RequestBlockLocation)
              .setRequestBlockLocationMsg(
                ControlMessage.RequestBlockLocationMsg.newBuilder()
                  .setExecutorId(executorId)
                  .setBlockIdWildcard(blockIdWildcard)
                  .build())
              .build());
        return responseFromMasterFuture;
      });
    blockLocationFuture.whenComplete((message, throwable) -> {
      pendingBlockLocationRequest.remove(blockIdWildcard);
    });

    // Using thenCompose so that fetching block data starts after getting response from master.
    return blockLocationFuture.thenCompose(responseFromMaster -> {
      if (responseFromMaster.getType() != ControlMessage.MessageType.BlockLocationInfo) {
        throw new RuntimeException("Response message type mismatch!");
      }

      final ControlMessage.BlockLocationInfoMsg blockLocationInfoMsg =
        responseFromMaster.getBlockLocationInfoMsg();
      if (!blockLocationInfoMsg.hasOwnerExecutorId()) {
        throw new BlockFetchException(new Throwable(
          "Block " + blockIdWildcard + " location unknown: "
            + "The block state is " + blockLocationInfoMsg.getState()));
      }

      // This is the executor id that we wanted to know
      final String blockId = blockLocationInfoMsg.getBlockId();
      final String targetExecutorId = blockLocationInfoMsg.getOwnerExecutorId();
      if (targetExecutorId.equals(executorId) || targetExecutorId.equals(REMOTE_FILE_STORE)) {
        // Block resides in the evaluator
        return getDataFromLocalBlock(blockId, blockStore, keyRange);
      } else {
        final ControlMessage.BlockTransferContextDescriptor descriptor =
          ControlMessage.BlockTransferContextDescriptor.newBuilder()
            .setBlockId(blockId)
            .setBlockStore(convertBlockStore(blockStore))
            .setRuntimeEdgeId(runtimeEdgeId)
            .setKeyRange(ByteString.copyFrom(SerializationUtils.serialize(keyRange)))
            .build();
        final CompletableFuture<ByteInputContext> contextFuture = blockTransferThrottler
          .requestTransferPermission(runtimeEdgeId)
          .thenCompose(obj -> byteTransfer.newInputContext(targetExecutorId, descriptor.toByteArray(), false));

        // whenComplete() ensures that blockTransferThrottler.onTransferFinished() is always called,
        // even on failures. Actual failure handling and Task retry will be done by DataFetcher.
        contextFuture.whenComplete((connectionContext, connectionThrowable) -> {
          if (connectionThrowable != null) {
            // Something wrong with the connection. Notify blockTransferThrottler immediately.
            blockTransferThrottler.onTransferFinished(runtimeEdgeId);
          } else {
            // Connection is okay. Notify blockTransferThrottler when the actual transfer is done, or fails.
            connectionContext.getCompletedFuture().whenComplete((transferContext, transferThrowable) -> {
              blockTransferThrottler.onTransferFinished(runtimeEdgeId);
            });
          }
        });

        return contextFuture
          .thenApply(context -> new DataUtil.InputStreamIterator(context.getInputStreams(),
            serializerManager.getSerializer(runtimeEdgeId)));
      }
    });
  }

  /**
   * Writes a block to a store.
   *
   * @param block             the block to write.
   * @param blockStore        the store to save the block.
   * @param expectedReadTotal the expected number of read for this block.
   * @param persistence       how to handle the used block.
   */
  public void writeBlock(final Block block,
                         final DataStoreProperty.Value blockStore,
                         final int expectedReadTotal,
                         final DataPersistenceProperty.Value persistence) {
    final String blockId = block.getId();
    LOG.info("CommitBlock: {}", blockId);

    switch (persistence) {
      case Discard:
        blockToRemainingRead.put(block.getId(), new AtomicInteger(expectedReadTotal));
        break;
      case Keep:
        // Do nothing but just keep the data.
        break;
      default:
        throw new UnsupportedExecutionPropertyException("This used data handling property is not supported.");
    }

    final BlockStore store = getBlockStore(blockStore);
    store.writeBlock(block);
    final ControlMessage.BlockStateChangedMsg.Builder blockStateChangedMsgBuilder =
      ControlMessage.BlockStateChangedMsg.newBuilder()
        .setExecutorId(executorId)
        .setBlockId(blockId)
        .setState(ControlMessage.BlockStateFromExecutor.AVAILABLE);

    if (DataStoreProperty.Value.GlusterFileStore.equals(blockStore)) {
      blockStateChangedMsgBuilder.setLocation(REMOTE_FILE_STORE);
    } else {
      blockStateChangedMsgBuilder.setLocation(executorId);
    }

    persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID)
      .send(ControlMessage.Message.newBuilder()
        .setId(RuntimeIdManager.generateMessageId())
        .setListenerId(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID)
        .setType(ControlMessage.MessageType.BlockStateChanged)
        .setBlockStateChangedMsg(blockStateChangedMsgBuilder.build())
        .build());
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
    final boolean deleted = store.deleteBlock(blockId);

    if (deleted) {
      final ControlMessage.BlockStateChangedMsg.Builder blockStateChangedMsgBuilder =
        ControlMessage.BlockStateChangedMsg.newBuilder()
          .setExecutorId(executorId)
          .setBlockId(blockId)
          .setState(ControlMessage.BlockStateFromExecutor.NOT_AVAILABLE);

      if (DataStoreProperty.Value.GlusterFileStore.equals(blockStore)) {
        blockStateChangedMsgBuilder.setLocation(REMOTE_FILE_STORE);
      } else {
        blockStateChangedMsgBuilder.setLocation(executorId);
      }

      persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID)
        .send(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(MessageEnvironment.BLOCK_MANAGER_MASTER_MESSAGE_LISTENER_ID)
          .setType(ControlMessage.MessageType.BlockStateChanged)
          .setBlockStateChangedMsg(blockStateChangedMsgBuilder)
          .build());
    } else {
      throw new BlockFetchException(new Throwable("Cannot find corresponding block " + blockId));
    }
  }

  //////////////////////////////////////////////////////////// Public methods for remote block I/O

  /**
   * Respond to a block request by another executor.
   * <p>
   * This method is executed by {org.apache.nemo.runtime.executor.data.blocktransfer.BlockTransport} thread. \
   * Never execute a blocking call in this method!
   *
   * @param outputContext {@link ByteOutputContext}
   * @throws InvalidProtocolBufferException from errors during parsing context descriptor
   */
  public void onOutputContext(final ByteOutputContext outputContext) throws InvalidProtocolBufferException {
    final ControlMessage.BlockTransferContextDescriptor descriptor =
      ControlMessage.BlockTransferContextDescriptor.PARSER.parseFrom(outputContext.getContextDescriptor());
    final DataStoreProperty.Value blockStore = convertBlockStore(descriptor.getBlockStore());
    final String blockId = descriptor.getBlockId();
    final KeyRange keyRange = SerializationUtils.deserialize(descriptor.getKeyRange().toByteArray());

    backgroundExecutorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          final Optional<Block> optionalBlock = getBlockStore(blockStore).readBlock(blockId);
          if (optionalBlock.isPresent()) {
            if (DataStoreProperty.Value.LocalFileStore.equals(blockStore)
              || DataStoreProperty.Value.GlusterFileStore.equals(blockStore)) {
              final List<FileArea> fileAreas = ((FileBlock) optionalBlock.get()).asFileAreas(keyRange);
              for (final FileArea fileArea : fileAreas) {
                try (ByteOutputContext.ByteOutputStream os = outputContext.newOutputStream()) {
                  os.writeFileArea(fileArea);
                }
              }
            } else {
              final Iterable<SerializedPartition> partitions = optionalBlock.get().readSerializedPartitions(keyRange);
              for (final SerializedPartition partition : partitions) {
                try (ByteOutputContext.ByteOutputStream os = outputContext.newOutputStream()) {
                  if (optionalBlock.get().getClass() == SerializedMemoryBlock.class) {
                    os.writeSerializedPartitionBuffer(partition, false);
                  } else {
                    // For NonSerializedMemoryBlock, the serialized partition to be sent is transient and needs
                    // to be released right after the data transfer.
                    os.writeSerializedPartitionBuffer(partition, true);
                  }
                }
              }
            }
            handleDataPersistence(blockStore, blockId);
            outputContext.close();

          } else {
            // We don't have the block here...
            throw new RuntimeException(String.format("Block %s not found in local BlockManagerWorker", blockId));
          }
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
   * This method is executed by {org.apache.nemo.runtime.executor.data.blocktransfer.BlockTransport}
   * thread. Never execute a blocking call in this method!
   *
   * @param inputContext {@link ByteInputContext}
   */
  public void onInputContext(final ByteInputContext inputContext) {
    throw new IllegalStateException("No logic here");
  }

  //////////////////////////////////////////////////////////// Private helper methods

  /**
   * Retrieves data from the stored block. A specific hash value range can be designated.
   *
   * @param blockId    of the block.
   * @param blockStore for the data storage.
   * @param keyRange   the key range descriptor.
   * @return the result data in the block.
   */
  private CompletableFuture<DataUtil.IteratorWithNumBytes> getDataFromLocalBlock(
    final String blockId,
    final DataStoreProperty.Value blockStore,
    final KeyRange keyRange) {
    final BlockStore store = getBlockStore(blockStore);

    // First, try to fetch the block from local BlockStore.
    final Optional<Block> optionalBlock = store.readBlock(blockId);

    if (optionalBlock.isPresent()) {
      final Iterable<NonSerializedPartition> partitions = optionalBlock.get().readPartitions(keyRange);
      handleDataPersistence(blockStore, blockId);

      // Block resides in this evaluator!
      try {
        final Iterator innerIterator = DataUtil.concatNonSerPartitions(partitions).iterator();
        long numSerializedBytes = 0;
        long numEncodedBytes = 0;
        try {
          for (final NonSerializedPartition partition : partitions) {
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
   * Handles used {@link org.apache.nemo.runtime.executor.data.block.Block}.
   *
   * @param blockStore the store which contains the block.
   * @param blockId    the ID of the block.
   */
  private void handleDataPersistence(final DataStoreProperty.Value blockStore,
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

  //////////////////////////////////////////////////////////// Converters

  /**
   * Gets the {@link BlockStore} from annotated value of {@link DataStoreProperty}.
   *
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
   * Decodes BlockStore property from protocol buffer.
   *
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
   *
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
