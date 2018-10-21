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
package org.apache.nemo.runtime.executor.data;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.KeyRange;
import org.apache.nemo.common.exception.BlockFetchException;
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.comm.ControlMessage.ByteTransferContextDescriptor;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.executor.bytetransfer.ByteInputContext;
import org.apache.nemo.runtime.executor.bytetransfer.ByteOutputContext;
import org.apache.nemo.runtime.executor.bytetransfer.ByteTransfer;
import org.apache.nemo.runtime.executor.data.partition.SerializedPartition;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Executor-side block manager.
 */
@ThreadSafe
public final class PipeManagerWorker {
  private static final Logger LOG = LoggerFactory.getLogger(PipeManagerWorker.class.getName());

  private final String executorId;
  private final SerializerManager serializerManager;

  // To-Executor connections
  private final ByteTransfer byteTransfer;

  @Inject
  private PipeManagerWorker(@Parameter(JobConf.ExecutorId.class) final String executorId,
                            final ByteTransfer byteTransfer,
                            final SerializerManager serializerManager) {
    this.executorId = executorId;
    this.byteTransfer = byteTransfer;
    this.serializerManager = serializerManager;
  }

  //////////////////////////////////////////////////////////// Main public methods

  public void onInputContext(final ByteInputContext inputContext) {
    // TODO: Receiver waits for sth that may have not come yet.

    final ByteTransferContextDescriptor descriptor = ByteTransferContextDescriptor.PARSER
      .parseFrom(outputContext.getContextDescriptor());
    descriptor.getBlockId(); // Exact destination and stuff
    descriptor.getRuntimeEdgeId();
    inputContext.getContextDescriptor()
    inputContext.getInputStreams();
  }


  public void write(final String runtimeEdgeId, final Object element) {
    // TODO: Sender just sends XX assuming that receivers are there waiting

    // (1) Get locations of destination tasks

    // (2) partitioning (in outputwriter?)

    // (3) Write to dst tasks
    final ByteTransferContextDescriptor descriptor = ByteTransferContextDescriptor.newBuilder()
      .setRuntimeEdgeId(runtimeEdgeId)
      .build();
    final CompletableFuture<ByteOutputContext> outputContext =
      byteTransfer.newOutputContext(targetExecutorId, descriptor.toByteArray());

    final ByteOutputContext.ByteOutputStream outputStream = contextFuture.get().newOutputStream();

    // Writes and flushes (element-wise)
    outputStream.write(element);



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

  public CompletableFuture<DataUtil.IteratorWithNumBytes> readPipe(final String runtimeEdgeId) {
    // Location...?

    // Let's see if a remote worker has it

    // Using thenCompose so that fetching block data starts after getting response from master.
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
    final ByteTransferContextDescriptor descriptor = ByteTransferContextDescriptor.PARSER
        .parseFrom(outputContext.getContextDescriptor());
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
                  os.writeSerializedPartition(partition);
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

}
