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

import edu.snu.onyx.runtime.exception.PartitionFetchException;
import edu.snu.onyx.runtime.exception.PartitionWriteException;
import edu.snu.onyx.runtime.executor.data.Block;
import edu.snu.onyx.runtime.executor.data.DataUtil;
import edu.snu.onyx.runtime.executor.data.HashRange;
import edu.snu.onyx.runtime.executor.data.partition.Partition;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This abstract class represents {@link PartitionStore}
 * which contains the (meta)data of the {@link Partition}s in local.
 * Because of this, store can maintain all partitions in a single map (mapped with their IDs).
 */
public abstract class LocalPartitionStore implements PartitionStore {
  // A map between partition id and data blocks.
  private final ConcurrentHashMap<String, Partition> partitionMap;

  protected LocalPartitionStore() {
    this.partitionMap = new ConcurrentHashMap<>();
  }

  /**
   * @see PartitionStore#getElements(String, HashRange).
   */
  @Override
  public final Optional<Iterable> getElements(final String partitionId,
                                              final HashRange hashRange) {
    final Partition partition = partitionMap.get(partitionId);

    if (partition != null) {
      try {
        final Iterable<Block> blocks = partition.getBlocks(hashRange);
        return Optional.of(DataUtil.concatBlocks(blocks));
      } catch (final IOException e) {
        throw new PartitionFetchException(e);
      }
    } else {
      return Optional.empty();
    }
  }

  /**
   * @see PartitionStore#putBlocks(String, Iterable, boolean).
   */
  @Override
  public final Optional<List<Long>> putBlocks(final String partitionId,
                                              final Iterable<Block> blocks,
                                              final boolean commitPerBlock) throws PartitionWriteException {
    try {
      final Partition partition = partitionMap.get(partitionId);
      if (partition == null) {
        throw new PartitionWriteException(new Throwable("The partition " + partitionId + "is not created yet."));
      }
      return partition.putBlocks(blocks);
    } catch (final IOException e) {
      // The partition is committed already.
      throw new PartitionWriteException(new Throwable("This partition is already committed."));
    }
  }

  /**
   * @see PartitionStore#commitPartition(String).
   */
  @Override
  public final void commitPartition(final String partitionId) {
    final Partition partition = partitionMap.get(partitionId);
    if (partition != null) {
      partition.commit();
    } else {
      throw new PartitionWriteException(new Throwable("There isn't any partition with id " + partitionId));
    }
  }

  /**
   * @return the map between the IDs and {@link Partition}.
   */
  public final ConcurrentHashMap<String, Partition> getPartitionMap() {
    return partitionMap;
  }
}
