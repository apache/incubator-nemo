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
package edu.snu.vortex.runtime.executor.block;

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.compiler.frontend.Coder;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.comm.ControlMessage;
import edu.snu.vortex.runtime.exception.NodeConnectionException;
import edu.snu.vortex.runtime.exception.UnsupportedBlockStoreException;
import edu.snu.vortex.runtime.executor.PersistentConnectionToMaster;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Executor-side block manager.
 */
@ThreadSafe
public final class BlockManagerWorker {
  private static final Logger LOG = Logger.getLogger(BlockManagerWorker.class.getName());

  public static final String NO_REMOTE_BLOCK = "";

  private final String executorId;

  private final LocalStore localStore;

  private final PersistentConnectionToMaster persistentConnectionToMaster;

  private final ConcurrentMap<String, Coder> runtimeEdgeIdToCoder;

  private final BlockTransferPeer blockTransferPeer;

  @Inject
  public BlockManagerWorker(@Parameter(JobConf.ExecutorId.class) final String executorId,
                            final LocalStore localStore,
                            final PersistentConnectionToMaster persistentConnectionToMaster,
                            final BlockTransferPeer blockTransferPeer) {
    this.executorId = executorId;
    this.localStore = localStore;
    this.persistentConnectionToMaster = persistentConnectionToMaster;
    this.runtimeEdgeIdToCoder = new ConcurrentHashMap<>();
    this.blockTransferPeer = blockTransferPeer;
  }

  /**
   * Return the coder for the specified runtime edge.
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
   * @param runtimeEdgeId id of the runtime edge
   * @param coder the corresponding coder
   */
  public void registerCoder(final String runtimeEdgeId, final Coder coder) {
    runtimeEdgeIdToCoder.putIfAbsent(runtimeEdgeId, coder);
  }

  public Iterable<Element> removeBlock(final String blockId,
                                       final RuntimeAttribute blockStore) {
    LOG.log(Level.INFO, "RemoveBlock: {0}", blockId);
    final BlockStore store = getBlockStore(blockStore);
    final Optional<Iterable<Element>> optional = store.removeBlock(blockId);
    if (!optional.isPresent()) {
      throw new RuntimeException("Trying to remove a non-existent block");
    }

    persistentConnectionToMaster.getMessageSender().send(
        ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setType(ControlMessage.MessageType.BlockStateChanged)
            .setBlockStateChangedMsg(
                ControlMessage.BlockStateChangedMsg.newBuilder()
                    .setExecutorId(executorId)
                    .setBlockId(blockId)
                    .setState(ControlMessage.BlockStateFromExecutor.REMOVED)
                    .build())
            .build());

    return optional.get();
  }


  /**
   * Store block somewhere.
   * Invariant: This should be invoked only once per blockId.
   * @param blockId of the block
   * @param data of the block
   * @param blockStore for storing the block
   */
  public void putBlock(final String blockId,
                       final Iterable<Element> data,
                       final RuntimeAttribute blockStore) {
    LOG.log(Level.INFO, "PutBlock: {0}", blockId);
    final BlockStore store = getBlockStore(blockStore);
    store.putBlock(blockId, data);

    persistentConnectionToMaster.getMessageSender().send(
        ControlMessage.Message.newBuilder()
            .setId(RuntimeIdGenerator.generateMessageId())
            .setType(ControlMessage.MessageType.BlockStateChanged)
            .setBlockStateChangedMsg(
                ControlMessage.BlockStateChangedMsg.newBuilder()
                    .setExecutorId(executorId)
                    .setBlockId(blockId)
                    .setState(ControlMessage.BlockStateFromExecutor.COMMITTED)
                    .build())
            .build());
  }

  /**
   * Get the stored block.
   * Unlike putBlock, this can be invoked multiple times per blockId (maybe due to failures).
   * Here, we first check if we have the block here, and then try to fetch the block from a remote worker.
   * @param blockId of the block
   * @param runtimeEdgeId id of the runtime edge that corresponds to the block
   * @param blockStore for the data storage
   * @return the block data
   */
  public Iterable<Element> getBlock(final String blockId,
                                    final String runtimeEdgeId,
                                    final RuntimeAttribute blockStore) {
    LOG.log(Level.INFO, "GetBlock: {0}", blockId);
    // Local hit!
    final BlockStore store = getBlockStore(blockStore);
    final Optional<Iterable<Element>> optionalData = store.getBlock(blockId);
    if (optionalData.isPresent()) {
      return optionalData.get();
    } else {
      // We don't have the block here... let's see if a remote worker has it
      // Ask Master for the location
      final ControlMessage.Message responseFromMaster;
      try {
        responseFromMaster = persistentConnectionToMaster.getMessageSender().<ControlMessage.Message>request(
            ControlMessage.Message.newBuilder()
                .setId(RuntimeIdGenerator.generateMessageId())
                .setType(ControlMessage.MessageType.RequestBlockLocation)
                .setRequestBlockLocationMsg(
                    ControlMessage.RequestBlockLocationMsg.newBuilder()
                        .setExecutorId(executorId)
                        .setBlockId(blockId)
                        .build())
                .build()).get();
      } catch (Exception e) {
        throw new NodeConnectionException(e);
      }

      final ControlMessage.BlockLocationInfoMsg blockLocationInfoMsg = responseFromMaster.getBlockLocationInfoMsg();
      assert (responseFromMaster.getType() == ControlMessage.MessageType.BlockLocationInfo);
      if (blockLocationInfoMsg.getOwnerExecutorId().equals(NO_REMOTE_BLOCK)) {
        // TODO #163: Handle Fault Tolerance
        // We should report this exception to the master, instead of shutting down the JVM
        throw new RuntimeException("Block " + blockId + " not found both in the local storage and the remote storage");
      }
      final String remoteWorkerId = blockLocationInfoMsg.getOwnerExecutorId();

      try {
        // TODO #250: Fetch multiple blocks in parallel
        return blockTransferPeer.fetch(remoteWorkerId, blockId, runtimeEdgeId, blockStore).get();
      } catch (final InterruptedException | ExecutionException e) {
        // TODO #163: Handle Fault Tolerance
        throw new NodeConnectionException(e);
      }
    }
  }

  private BlockStore getBlockStore(final RuntimeAttribute blockStore) {
    switch (blockStore) {
      case Local:
        return localStore;
      case Memory:
        // TODO #181: Implement MemoryBlockStore
        return localStore;
      case File:
        // TODO #69: Implement file channel in Runtime
        return localStore;
      case MemoryFile:
        // TODO #69: Implement file channel in Runtime
        return localStore;
      case DistributedStorage:
        // TODO #180: Implement DistributedStorageStore
        return localStore;
      default:
        throw new UnsupportedBlockStoreException(new Exception(blockStore + " is not supported."));
    }
  }

}
