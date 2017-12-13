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
package edu.snu.onyx.runtime.executor.data.stores;

import edu.snu.onyx.common.exception.BlockFetchException;
import edu.snu.onyx.common.exception.BlockWriteException;
import edu.snu.onyx.runtime.common.data.HashRange;
import edu.snu.onyx.runtime.executor.data.BlockManagerWorker;
import edu.snu.onyx.runtime.executor.data.NonSerializedPartition;
import edu.snu.onyx.runtime.executor.data.SerializedPartition;
import edu.snu.onyx.runtime.executor.data.block.Block;
import org.apache.reef.tang.InjectionFuture;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This abstract class represents {@link BlockStore}
 * which contains the (meta)data of the {@link Block}s in local.
 * Because of this, store can maintain all blocks in a single map (mapped with their IDs).
 */
public abstract class LocalBlockStore extends AbstractBlockStore {
  // A map between block id and data blocks.
  private final ConcurrentHashMap<String, Block> blockMap;

  protected LocalBlockStore(final InjectionFuture<BlockManagerWorker> blockManagerWorker) {
    super(blockManagerWorker);
    this.blockMap = new ConcurrentHashMap<>();
  }

  /**
   * @see BlockStore#putPartitions(String, Iterable, boolean).
   */
  @Override
  public final Optional<List<Long>> putPartitions(final String blockId,
                                                  final Iterable<NonSerializedPartition> partitions,
                                                  final boolean commitPerPartition) throws BlockWriteException {
    try {
      final Block block = blockMap.get(blockId);
      if (block == null) {
        throw new BlockWriteException(new Throwable("The block " + blockId + "is not created yet."));
      }
      return block.putPartitions(partitions);
    } catch (final IOException e) {
      throw new BlockWriteException(new Throwable("Failed to store partitions to this block."));
    }
  }

  /**
   * @see BlockStore#putSerializedPartitions(String, Iterable, boolean).
   */
  @Override
  public final List<Long> putSerializedPartitions(final String blockId,
                                                  final Iterable<SerializedPartition> partitions,
                                                  final boolean commitPerPartition) {
    try {
      final Block block = blockMap.get(blockId);
      if (block == null) {
        throw new BlockWriteException(new Throwable("The block " + blockId + "is not created yet."));
      }
      return block.putSerializedPartitions(partitions);
    } catch (final IOException e) {
      throw new BlockWriteException(new Throwable("Failed to store partitions to this block."));
    }
  }

  /**
   * @see BlockStore#getPartitions(String, HashRange).
   */
  @Override
  public final Optional<Iterable<NonSerializedPartition>> getPartitions(final String blockId,
                                                                        final HashRange hashRange) {
    final Block block = blockMap.get(blockId);

    if (block != null) {
      try {
        final Iterable<NonSerializedPartition> partitionsInRange = block.getPartitions(hashRange);
        return Optional.of(partitionsInRange);
      } catch (final IOException e) {
        throw new BlockFetchException(e);
      }
    } else {
      return Optional.empty();
    }
  }

  /**
   * @see BlockStore#getSerializedPartitions(String, HashRange).
   */
  @Override
  public final Optional<Iterable<SerializedPartition>> getSerializedPartitions(final String blockId,
                                                                               final HashRange hashRange) {
    final Block block = blockMap.get(blockId);

    if (block != null) {
      try {
        final Iterable<SerializedPartition> partitionsInRange = block.getSerializedPartitions(hashRange);
        return Optional.of(partitionsInRange);
      } catch (final IOException e) {
        throw new BlockFetchException(e);
      }
    } else {
      return Optional.empty();
    }
  }

  /**
   * @see BlockStore#commitBlock(String).
   */
  @Override
  public final void commitBlock(final String blockId) {
    final Block block = blockMap.get(blockId);
    if (block != null) {
      block.commit();
    } else {
      throw new BlockWriteException(new Throwable("There isn't any block with id " + blockId));
    }
  }

  /**
   * @return the map between the IDs and {@link Block}.
   */
  public final ConcurrentHashMap<String, Block> getBlockMap() {
    return blockMap;
  }
}
