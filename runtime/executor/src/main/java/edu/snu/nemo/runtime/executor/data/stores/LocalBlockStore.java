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
package edu.snu.nemo.runtime.executor.data.stores;

import edu.snu.nemo.common.exception.BlockFetchException;
import edu.snu.nemo.common.exception.BlockWriteException;
import edu.snu.nemo.runtime.common.data.KeyRange;
import edu.snu.nemo.runtime.executor.data.SerializerManager;
import edu.snu.nemo.runtime.executor.data.NonSerializedPartition;
import edu.snu.nemo.runtime.executor.data.SerializedPartition;
import edu.snu.nemo.runtime.executor.data.block.Block;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This abstract class represents {@link BlockStore}
 * which contains the (meta)data of the {@link Block}s in local.
 * Because of this, store can maintain all blocks in a single map (mapped with their IDs).
 */
public abstract class LocalBlockStore extends AbstractBlockStore {
  // A map between block id and data blocks.
  private final Map<String, Block> blockMap;

  /**
   * Constructor.
   *
   * @param coderManager the coder manager.
   */
  protected LocalBlockStore(final SerializerManager coderManager) {
    super(coderManager);
    this.blockMap = new ConcurrentHashMap<>();
  }

  /**
   * @see BlockStore#putPartitions(String, Iterable)
   */
  @Override
  public final <K extends Serializable>
  Optional<List<Long>> putPartitions(final String blockId,
                                     final Iterable<NonSerializedPartition<K>> partitions)
      throws BlockWriteException {
    try {
      final Block<K> block = blockMap.get(blockId);
      if (block == null) {
        throw new BlockWriteException(new Throwable("The block " + blockId + "is not created yet."));
      }
      return block.putPartitions(partitions);
    } catch (final IOException e) {
      throw new BlockWriteException(new Throwable("Failed to store partitions to this block."));
    }
  }

  /**
   * @see BlockStore#putSerializedPartitions(String, Iterable)
   */
  @Override
  public final <K extends Serializable>
  List<Long> putSerializedPartitions(final String blockId,
                                     final Iterable<SerializedPartition<K>> partitions) {
    try {
      final Block<K> block = blockMap.get(blockId);
      if (block == null) {
        throw new BlockWriteException(new Throwable("The block " + blockId + "is not created yet."));
      }
      return block.putSerializedPartitions(partitions);
    } catch (final IOException e) {
      throw new BlockWriteException(new Throwable("Failed to store partitions to this block."));
    }
  }

  /**
   * @see BlockStore#getPartitions(String, KeyRange)
   */
  @Override
  public final <K extends Serializable>
  Optional<Iterable<NonSerializedPartition<K>>> getPartitions(final String blockId, final KeyRange<K> keyRange) {
    final Block<K> block = blockMap.get(blockId);

    if (block != null) {
      try {
        final Iterable<NonSerializedPartition<K>> partitionsInRange = block.getPartitions(keyRange);
        return Optional.of(partitionsInRange);
      } catch (final IOException e) {
        throw new BlockFetchException(e);
      }
    } else {
      return Optional.empty();
    }
  }

  /**
   * @see BlockStore#getSerializedPartitions(String, edu.snu.nemo.runtime.common.data.KeyRange)
   */
  @Override
  public final <K extends Serializable>
  Optional<Iterable<SerializedPartition<K>>> getSerializedPartitions(final String blockId, final KeyRange<K> keyRange) {
    final Block<K> block = blockMap.get(blockId);

    if (block != null) {
      try {
        final Iterable<SerializedPartition<K>> partitionsInRange = block.getSerializedPartitions(keyRange);
        return Optional.of(partitionsInRange);
      } catch (final IOException e) {
        throw new BlockFetchException(e);
      }
    } else {
      return Optional.empty();
    }
  }

  /**
   * @see BlockStore#commitBlock(String)
   */
  @Override
  public final void commitBlock(final String blockId) {
    final Block block = blockMap.get(blockId);
    if (block != null) {
      try {
        block.commit();
      } catch (final IOException e) {
        throw new BlockWriteException(e);
      }
    } else {
      throw new BlockWriteException(new Throwable("There isn't any block with id " + blockId));
    }
  }

  /**
   * @return the map between the IDs and {@link Block}.
   */
  public final Map<String, Block> getBlockMap() {
    return blockMap;
  }
}
