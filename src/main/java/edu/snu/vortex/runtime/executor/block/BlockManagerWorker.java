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

import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.state.BlockState;
import edu.snu.vortex.runtime.exception.UnsupportedBlockStoreException;
import edu.snu.vortex.runtime.master.BlockManagerMaster;

import javax.annotation.concurrent.ThreadSafe;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Executor-side block manager.
 * For now, all its operations are synchronized to guarantee thread safety.
 */
@ThreadSafe
public final class BlockManagerWorker {
  private final String workerId;

  private final BlockManagerMaster blockManagerMaster;

  private final LocalStore localStore;

  private final Set<String> idOfBlocksStoredInThisWorker;

  public BlockManagerWorker(final String workerId,
                            final BlockManagerMaster blockManagerMaster,
                            final LocalStore localStore) {
    this.workerId = workerId;
    this.blockManagerMaster = blockManagerMaster;
    this.localStore = localStore;
    this.idOfBlocksStoredInThisWorker = new HashSet<>();
  }

  public String getWorkerId() {
    return workerId;
  }

  /**
   * Store block somewhere.
   * Invariant: This should be invoked only once per blockId.
   * @param blockId of the block
   * @param data of the block
   * @param blockStore for storing the block
   */
  public synchronized void putBlock(final String blockId,
                                    final Iterable<Element> data,
                                    final RuntimeAttribute blockStore) {
    if (idOfBlocksStoredInThisWorker.contains(blockId)) {
      throw new RuntimeException("Trying to put an already existing block");
    }
    final BlockStore store = getBlockStore(blockStore);
    store.putBlock(blockId, data);
    idOfBlocksStoredInThisWorker.add(blockId);

    // TODO #186: Integrate BlockManager Master/Workers with Protobuf Messages
    blockManagerMaster.onBlockStateChanged(workerId, blockId, BlockState.State.COMMITTED);
  }

  /**
   * Get the stored block.
   * Unlike putBlock, this can be invoked multiple times per blockId (maybe due to failures).
   * Here, we first check if we have the block here, and then try to fetch the block from a remote worker.
   * @param blockId of the block
   * @param blockStore for the data storage
   * @return the block data
   */
  public synchronized Iterable<Element> getBlock(final String blockId, final RuntimeAttribute blockStore) {
    if (idOfBlocksStoredInThisWorker.contains(blockId)) {
      // Local hit!
      final BlockStore store = getBlockStore(blockStore);
      final Optional<Iterable<Element>> optionalData = store.getBlock(blockId);
      if (optionalData.isPresent()) {
        return optionalData.get();
      } else {
        // TODO #163: Handle Fault Tolerance
        // We should report this exception to the master, instead of shutting down the JVM
        throw new RuntimeException("Something's wrong: worker thinks it has the block, but the store doesn't have it");
      }
    } else {
      // We don't have the block here... let's see if a remote worker has it
      // This part should be replaced with an RPC.
      // TODO #186: Integrate BlockManager Master/Workers with Protobuf Messages
      final Optional<BlockManagerWorker> optionalWorker = blockManagerMaster.getBlockLocation(blockId);
      if (optionalWorker.isPresent()) {
        final BlockManagerWorker remoteWorker = optionalWorker.get();
        final Optional<Iterable<Element>> optionalData = remoteWorker.getBlockRemotely(workerId, blockId, blockStore);
        if (optionalData.isPresent()) {
          return optionalData.get();
        } else {
          // TODO #163: Handle Fault Tolerance
          // We should report this exception to the master, instead of shutting down the JVM
          throw new RuntimeException("Failed fetching block " + blockId + "from worker " + remoteWorker.getWorkerId());
        }
      } else {
        // TODO #163: Handle Fault Tolerance
        // We should report this exception to the master, instead of shutting down the JVM
        throw new RuntimeException("Block " + blockId + " not found both in the local storage and the remote storage");
      }
    }
  }

  /**
   * Get block request from a remote worker.
   * Should be replaced with an RPC.
   * TODO #186: Integrate BlockManager Master/Workers with Protobuf Messages
   * @param requestingWorkerId of the requestor
   * @param blockId to get
   * @param blockStore for the block
   * @return block data
   */
  public synchronized Optional<Iterable<Element>> getBlockRemotely(final String requestingWorkerId,
                                                                   final String blockId,
                                                                   final RuntimeAttribute blockStore) {
    final BlockStore store = getBlockStore(blockStore);
    return store.getBlock(blockId);
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
