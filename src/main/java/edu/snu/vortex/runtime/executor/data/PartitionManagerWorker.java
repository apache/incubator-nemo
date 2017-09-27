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
package edu.snu.vortex.runtime.executor.data;

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.exception.PartitionFetchException;
import edu.snu.vortex.runtime.exception.PartitionWriteException;
import edu.snu.vortex.runtime.exception.UnsupportedPartitionStoreException;
import edu.snu.vortex.runtime.executor.PersistentConnectionToMasterMap;
import edu.snu.vortex.runtime.executor.data.partitiontransfer.PartitionInputStream;
import edu.snu.vortex.runtime.executor.data.partitiontransfer.PartitionOutputStream;
import edu.snu.vortex.runtime.executor.data.partitiontransfer.PartitionTransfer;
import edu.snu.vortex.runtime.master.RuntimeMaster;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor-side partition manager.
 */
@ThreadSafe
public final class PartitionManagerWorker {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionManagerWorker.class.getName());
  private static final String REMOTE_FILE_STORE = "REMOTE_FILE_STORE";

  private final String executorId;

  private final MemoryStore memoryStore;

  private final LocalFileStore localFileStore;

  private final RemoteFileStore remoteFileStore;

  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;

  private final ConcurrentMap<String, Coder> runtimeEdgeIdToCoder;

  private final PartitionTransfer partitionTransfer;

  @Inject
  private PartitionManagerWorker(@Parameter(JobConf.ExecutorId.class) final String executorId,
                                 final MemoryStore memoryStore,
                                 final LocalFileStore localFileStore,
                                 final RemoteFileStore remoteFileStore,
                                 final PersistentConnectionToMasterMap persistentConnectionToMasterMap,
                                 final PartitionTransfer partitionTransfer) {
    this.executorId = executorId;
    this.memoryStore = memoryStore;
    this.localFileStore = localFileStore;
    this.remoteFileStore = remoteFileStore;
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.runtimeEdgeIdToCoder = new ConcurrentHashMap<>();
    this.partitionTransfer = partitionTransfer;
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
   * Retrieves data from the stored partition. A specific hash value range can be designated.
   * This can be invoked multiple times per partitionId (maybe due to failures).
   * Here, we first check if we have the partition here, and then try to fetch the partition from a remote worker.
   *
   * @param partitionId    of the partition.
   * @param runtimeEdgeId  id of the runtime edge that corresponds to the partition.
   * @param partitionStore for the data storage.
   * @param hashRange      the hash range descriptor
   * @return a {@link CompletableFuture} for the partition.
   */
  public CompletableFuture<Iterable<Element>> retrieveDataFromPartition(
      final String partitionId,
      final String runtimeEdgeId,
      final Class<? extends PartitionStore> partitionStore,
      final HashRange hashRange) {
    LOG.info("RetrieveDataFromPartition: {}", partitionId);
    final PartitionStore store = getPartitionStore(partitionStore);

    // First, try to fetch the partition from local PartitionStore.
    final Optional<CompletableFuture<Iterable<Element>>> optionalResultData = store.getBlocks(partitionId, hashRange);

    if (optionalResultData.isPresent()) {
      // Partition resides in this evaluator!
      return optionalResultData.get();
    } else if (partitionStore.equals(GlusterFileStore.class)) {
      throw new PartitionFetchException(new Throwable("Cannot find a partition in remote store."));
    } else {
      // We don't have the partition here...
      return requestPartitionInRemoteWorker(partitionId, runtimeEdgeId, partitionStore, hashRange);
    }
  }

  /**
   * Requests data in a specific hash value range from a partition which resides in a remote worker asynchronously.
   * If the hash value range is [0, int.max), it will retrieve the whole data from the partition.
   *
   * @param partitionId       of the partition.
   * @param runtimeEdgeId     id of the runtime edge that corresponds to the partition.
   * @param partitionStore    for the data storage.
   * @param hashRange         the hash range descriptor
   * @return the {@link CompletableFuture} of the partition.
   */
  private CompletableFuture<Iterable<Element>> requestPartitionInRemoteWorker(
      final String partitionId,
      final String runtimeEdgeId,
      final Class<? extends PartitionStore> partitionStore,
      final HashRange hashRange) {
    // Let's see if a remote worker has it
    // Ask Master for the location
    final CompletableFuture<ControlMessage.Message> responseFromMasterFuture = persistentConnectionToMasterMap
        .getMessageSender(MessageEnvironment.PARTITION_MANAGER_MASTER_MESSAGE_LISTENER_ID).request(
            ControlMessage.Message.newBuilder()
                .setId(RuntimeIdGenerator.generateMessageId())
                .setListenerId(MessageEnvironment.PARTITION_MANAGER_MASTER_MESSAGE_LISTENER_ID)
                .setType(ControlMessage.MessageType.RequestPartitionLocation)
                .setRequestPartitionLocationMsg(
                    ControlMessage.RequestPartitionLocationMsg.newBuilder()
                        .setExecutorId(executorId)
                        .setPartitionId(partitionId)
                        .build())
                .build());
    // Using thenCompose so that fetching partition data starts after getting response from master.
    return responseFromMasterFuture.thenCompose(responseFromMaster -> {
      assert (responseFromMaster.getType() == ControlMessage.MessageType.PartitionLocationInfo);
      final ControlMessage.PartitionLocationInfoMsg partitionLocationInfoMsg =
          responseFromMaster.getPartitionLocationInfoMsg();
      if (!partitionLocationInfoMsg.hasOwnerExecutorId()) {
        throw new PartitionFetchException(new Throwable(
            "Partition " + partitionId + " not found both in the local storage and the remote storage: The"
                + "partition state is " + RuntimeMaster.convertPartitionState(partitionLocationInfoMsg.getState())));
      }
      // This is the executor id that we wanted to know
      final String remoteWorkerId = partitionLocationInfoMsg.getOwnerExecutorId();
      return partitionTransfer.initiatePull(remoteWorkerId, false, partitionStore, partitionId,
          runtimeEdgeId, hashRange).getCompleteFuture();
    });
  }

  /**
   * Store an iterable of data blocks to a partition in the target {@code PartitionStore}.
   * Invariant: This should not be invoked after a partition is committed.
   *
   * @param partitionId    of the partition.
   * @param blocks         to save to a partition.
   * @param partitionStore to store the partition.
   * @param commitPerBlock whether commit every block write or not.
   * @return a {@link CompletableFuture} of the size of each written block.
   */
  public CompletableFuture<Optional<List<Long>>> putBlocks(final String partitionId,
                                                           final Iterable<Block> blocks,
                                                           final Class<? extends PartitionStore> partitionStore,
                                                           final boolean commitPerBlock) {
    LOG.info("PutBlocks: {}", partitionId);
    final PartitionStore store = getPartitionStore(partitionStore);

    try {
      return store.putBlocks(partitionId, blocks, commitPerBlock);
    } catch (final Exception e) {
      throw new PartitionWriteException(e);
    }
  }

  /**
   * Notifies that all writes for a partition is end.
   * Subscribers waiting for the data of the target partition are notified when the partition is committed.
   * Also, further subscription about a committed partition will not blocked but get the data in it and finished.
   *
   * @param partitionId    of the partition.
   * @param partitionStore to store the partition.
   * @param blockSizeInfo  the size metric of blocks.
   * @param srcIRVertexId  of the source task.
   * @param srcTaskIdx     of the source task.
   */
  public void commitPartition(final String partitionId,
                              final Class<? extends PartitionStore> partitionStore,
                              final List<Long> blockSizeInfo,
                              final String srcIRVertexId,
                              final int srcTaskIdx) {
    LOG.info("CommitPartition: {}", partitionId);
    final PartitionStore store = getPartitionStore(partitionStore);
    store.commitPartition(partitionId);
    final ControlMessage.PartitionStateChangedMsg.Builder partitionStateChangedMsgBuilder =
        ControlMessage.PartitionStateChangedMsg.newBuilder()
            .setExecutorId(executorId)
            .setPartitionId(partitionId)
            .setSrcTaskIdx(srcTaskIdx)
            .setState(ControlMessage.PartitionStateFromExecutor.COMMITTED);

    if (partitionStore == GlusterFileStore.class) {
      partitionStateChangedMsgBuilder.setLocation(REMOTE_FILE_STORE);
    } else {
      partitionStateChangedMsgBuilder.setLocation(executorId);
    }

    persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.PARTITION_MANAGER_MASTER_MESSAGE_LISTENER_ID)
        .send(ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setListenerId(MessageEnvironment.PARTITION_MANAGER_MASTER_MESSAGE_LISTENER_ID)
            .setType(ControlMessage.MessageType.PartitionStateChanged)
            .setPartitionStateChangedMsg(partitionStateChangedMsgBuilder.build())
            .build());

    if (!blockSizeInfo.isEmpty()) {
      // TODO #511: Refactor metric aggregation for (general) run-rime optimization.
      persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
          .send(ControlMessage.Message.newBuilder()
              .setId(RuntimeIdGenerator.generateMessageId())
              .setListenerId(MessageEnvironment.RUNTIME_MASTER_MESSAGE_LISTENER_ID)
              .setType(ControlMessage.MessageType.DataSizeMetric)
              .setDataSizeMetricMsg(ControlMessage.DataSizeMetricMsg.newBuilder()
                  .setPartitionId(partitionId)
                  .setSrcIRVertexId(srcIRVertexId)
                  .addAllBlockSizeInfo(blockSizeInfo)
              )
              .build());
    }
  }

  /**
   * Remove the partition from store.
   *
   * @param partitionId    of the partition to remove.
   * @param partitionStore tha the partition is stored.
   */
  public void removePartition(final String partitionId,
                              final Class<? extends PartitionStore> partitionStore) {
    LOG.info("RemovePartition: {}", partitionId);
    final PartitionStore store = getPartitionStore(partitionStore);
    final boolean exist;
    try {
      exist = store.removePartition(partitionId).get();
    } catch (final InterruptedException | ExecutionException e) {
      throw new PartitionFetchException(e);
    }

    if (exist) {
      final ControlMessage.PartitionStateChangedMsg.Builder partitionStateChangedMsgBuilder =
          ControlMessage.PartitionStateChangedMsg.newBuilder()
              .setExecutorId(executorId)
              .setPartitionId(partitionId)
              .setState(ControlMessage.PartitionStateFromExecutor.REMOVED);

      if (GlusterFileStore.class.equals(partitionStore)) {
        partitionStateChangedMsgBuilder.setLocation(REMOTE_FILE_STORE);
      } else {
        partitionStateChangedMsgBuilder.setLocation(executorId);
      }

      persistentConnectionToMasterMap.getMessageSender(MessageEnvironment.PARTITION_MANAGER_MASTER_MESSAGE_LISTENER_ID)
          .send(ControlMessage.Message.newBuilder()
              .setId(RuntimeIdGenerator.generateMessageId())
              .setListenerId(MessageEnvironment.PARTITION_MANAGER_MASTER_MESSAGE_LISTENER_ID)
              .setType(ControlMessage.MessageType.PartitionStateChanged)
              .setPartitionStateChangedMsg(partitionStateChangedMsgBuilder)
              .build());
    } else {
      throw new PartitionFetchException(new Throwable("Cannot find corresponding partition " + partitionId));
    }
  }

  private PartitionStore getPartitionStore(final Class<? extends PartitionStore> partitionStore) {
    switch (partitionStore.getSimpleName()) {
      case MemoryStore.SIMPLE_NAME:
        return memoryStore;
      case LocalFileStore.SIMPLE_NAME:
        return localFileStore;
      case GlusterFileStore.SIMPLE_NAME:
        return remoteFileStore;
      default:
        throw new UnsupportedPartitionStoreException(new Exception(partitionStore + " is not supported."));
    }
  }

  /**
   * Respond to a pull request by another executor.
   *
   * This method is executed by {@link edu.snu.vortex.runtime.executor.data.partitiontransfer.PartitionTransport}
   * thread. Never execute a blocking call in this method!
   *
   * @param outputStream {@link PartitionOutputStream}
   */
  public void onPullRequest(final PartitionOutputStream<?> outputStream) {
    // We are getting the partition from local store!
    final Optional<Class<? extends PartitionStore>> partitionStoreOptional = outputStream.getPartitionStore();
    final Class<? extends PartitionStore> partitionStore = partitionStoreOptional.get();
    if (partitionStore.equals(LocalFileStore.class) || partitionStore.equals(GlusterFileStore.class)) {
      // TODO #492: Modularize the data communication pattern. Remove execution property value dependant code.
      final FileStore fileStore = (FileStore) getPartitionStore(partitionStore);
      try {
        outputStream.writeFileAreas(fileStore.getFileAreas(outputStream.getPartitionId(),
            outputStream.getHashRange())).close();
      } catch (final IOException | PartitionFetchException e) {
        LOG.error("Closing a pull request exceptionally", e);
        outputStream.closeExceptionally(e);
      }
    } else {
      final CompletableFuture<Iterable<Element>> partitionFuture =
          retrieveDataFromPartition(outputStream.getPartitionId(), outputStream.getRuntimeEdgeId(),
              partitionStore, outputStream.getHashRange());
      partitionFuture.thenAcceptAsync(partition -> {
        try {
          outputStream.writeElements(partition).close();
        } catch (final IOException e) {
          LOG.error("Closing a pull request exceptionally", e);
          outputStream.closeExceptionally(e);
        }
      });
    }
  }

  /**
   * Respond to a push notification by another executor.
   *
   * A push notification is generated when a remote executor invokes {@link edu.snu.vortex.runtime.executor.data
   * .partitiontransfer.PartitionTransfer#initiateSend(String, boolean, String, String, HashRange)} to transfer
   * a partition to another executor.
   *
   * This method is executed by {@link edu.snu.vortex.runtime.executor.data.partitiontransfer.PartitionTransport}
   * thread. Never execute a blocking call in this method!
   *
   * @param inputStream {@link PartitionInputStream}
   */
  public void onPushNotification(final PartitionInputStream inputStream) {
  }
}
