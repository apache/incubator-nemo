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
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.exception.PartitionFetchException;
import edu.snu.vortex.runtime.exception.NodeConnectionException;
import edu.snu.vortex.runtime.exception.PartitionWriteException;
import edu.snu.vortex.runtime.exception.UnsupportedPartitionStoreException;
import edu.snu.vortex.runtime.executor.PersistentConnectionToMaster;
import edu.snu.vortex.runtime.executor.data.partition.Partition;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Executor-side partition manager.
 */
@ThreadSafe
public final class PartitionManagerWorker {
  private static final Logger LOG = Logger.getLogger(PartitionManagerWorker.class.getName());

  public static final String NO_REMOTE_PARTITION = "";

  private final String executorId;

  private final LocalStore localStore;

  private final FileStore fileStore;

  private final PersistentConnectionToMaster persistentConnectionToMaster;

  private final ConcurrentMap<String, Coder> runtimeEdgeIdToCoder;

  private final PartitionTransferPeer partitionTransferPeer;

  @Inject
  private PartitionManagerWorker(@Parameter(JobConf.ExecutorId.class) final String executorId,
                                 final LocalStore localStore,
                                 final FileStore fileStore,
                                 final PersistentConnectionToMaster persistentConnectionToMaster,
                                 final PartitionTransferPeer partitionTransferPeer) {
    this.executorId = executorId;
    this.localStore = localStore;
    this.fileStore = fileStore;
    this.persistentConnectionToMaster = persistentConnectionToMaster;
    this.runtimeEdgeIdToCoder = new ConcurrentHashMap<>();
    this.partitionTransferPeer = partitionTransferPeer;
  }

  /**
   * Return the coder for the specified runtime edge.
   *
   * @param runtimeEdgeId id of the runtime edge
   * @return the corresponding coder
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
   * @param runtimeEdgeId id of the runtime edge
   * @param coder         the corresponding coder
   */
  public void registerCoder(final String runtimeEdgeId, final Coder coder) {
    runtimeEdgeIdToCoder.putIfAbsent(runtimeEdgeId, coder);
  }

  /**
   * Remove the partition from store.
   *
   * @param partitionId of the partition to remove.
   * @param partitionStore tha the partition is stored.
   * @return whether the partition is removed or not.
   */
  public boolean removePartition(final String partitionId,
                                 final Attribute partitionStore) {
    LOG.log(Level.INFO, "RemovePartition: {0}", partitionId);
    final PartitionStore store = getPartitionStore(partitionStore);
    final boolean exist = store.removePartition(partitionId);

    if (exist) {
      persistentConnectionToMaster.getMessageSender().send(
          ControlMessage.Message.newBuilder()
              .setId(RuntimeIdGenerator.generateMessageId())
              .setType(ControlMessage.MessageType.PartitionStateChanged)
              .setPartitionStateChangedMsg(
                  ControlMessage.PartitionStateChangedMsg.newBuilder()
                      .setExecutorId(executorId)
                      .setPartitionId(partitionId)
                      .setState(ControlMessage.PartitionStateFromExecutor.REMOVED)
                      .build())
              .build());
    }

    return exist;
  }


  /**
   * Store partition somewhere.
   * Invariant: This should be invoked only once per partitionId.
   *
   * @param partitionId    of the partition
   * @param data           of the partition
   * @param partitionStore for storing the partition
   */
  public void putPartition(final String partitionId,
                           final Iterable<Element> data,
                           final Attribute partitionStore) {
    LOG.log(Level.INFO, "PutPartition: {0}", partitionId);
    final PartitionStore store = getPartitionStore(partitionStore);

    try {
      store.putPartition(partitionId, data);
    } catch (final Exception e) {
      throw new PartitionWriteException(e);
    }

    persistentConnectionToMaster.getMessageSender().send(
        ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setType(ControlMessage.MessageType.PartitionStateChanged)
            .setPartitionStateChangedMsg(
                ControlMessage.PartitionStateChangedMsg.newBuilder()
                    .setExecutorId(executorId)
                    .setPartitionId(partitionId)
                    .setState(ControlMessage.PartitionStateFromExecutor.COMMITTED)
                    .build())
            .build());
  }

  /**
   * Get the stored partition.
   * Unlike putPartition, this can be invoked multiple times per partitionId (maybe due to failures).
   * Here, we first check if we have the partition here, and then try to fetch the partition from a remote worker.
   *
   * @param partitionId    of the partition
   * @param runtimeEdgeId  id of the runtime edge that corresponds to the partition
   * @param partitionStore for the data storage
   * @return a {@link CompletableFuture} for the partition
   */
  public CompletableFuture<Partition> getPartition(final String partitionId,
                                                   final String runtimeEdgeId,
                                                   final Attribute partitionStore) {
    LOG.log(Level.INFO, "GetPartition: {0}", partitionId);
    final PartitionStore store = getPartitionStore(partitionStore);
    final Optional<Partition> optionalPartition;

    try {
      optionalPartition = store.getPartition(partitionId);
    } catch (final Exception e) {
      throw new PartitionFetchException(e);
    }

    if (optionalPartition.isPresent()) {
      // Local hit!
      return CompletableFuture.completedFuture(optionalPartition.get());
    }
    // We don't have the partition here... let's see if a remote worker has it
    // Ask Master for the location
    final Future<ControlMessage.Message> responseFromMasterFuture =
        persistentConnectionToMaster.getMessageSender().request(
            ControlMessage.Message.newBuilder()
                .setId(RuntimeIdGenerator.generateMessageId())
                .setType(ControlMessage.MessageType.RequestPartitionLocation)
                .setRequestPartitionLocationMsg(
                    ControlMessage.RequestPartitionLocationMsg.newBuilder()
                        .setExecutorId(executorId)
                        .setPartitionId(partitionId)
                        .build())
                .build());

    // Convert Future<ControlMessage.Message> to CompletableFuture<ControlMessage.Message>
    final CompletableFuture<ControlMessage.Message> responseFromMaster = CompletableFuture.supplyAsync(() -> {
      try {
        return responseFromMasterFuture.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new NodeConnectionException(e);
      }
    });

    // PartitionTransferPeer#fetch returns a CompletableFuture.
    // Composing two CompletableFuture so that fetching partition data starts after getting response from master.
    return responseFromMaster.thenCompose(response -> {
      assert (response.getType() == ControlMessage.MessageType.PartitionLocationInfo);
      final ControlMessage.PartitionLocationInfoMsg partitionLocationInfoMsg =
          response.getPartitionLocationInfoMsg();
      if (partitionLocationInfoMsg.getOwnerExecutorId().equals(NO_REMOTE_PARTITION)) {
        throw new PartitionFetchException(
            new Throwable("Partition " + partitionId + " not found both in the local storage and the remote storage"));
      }
      // This is the executor id that we wanted to know
      final String remoteWorkerId = partitionLocationInfoMsg.getOwnerExecutorId();
      return partitionTransferPeer.fetch(remoteWorkerId, partitionId, runtimeEdgeId, partitionStore);
    });

  }

  private PartitionStore getPartitionStore(final Attribute partitionStore) {
    switch (partitionStore) {
      case Local:
        return localStore;
      case Memory:
        // TODO #181: Implement MemoryPartitionStore
        return localStore;
      case File:
        return fileStore;
      case MemoryFile:
        // TODO #181: Implement MemoryPartitionStore
        return localStore;
      case DistributedStorage:
        // TODO #180: Implement DistributedStorageStore
        return localStore;
      default:
        throw new UnsupportedPartitionStoreException(new Exception(partitionStore + " is not supported."));
    }
  }
}
