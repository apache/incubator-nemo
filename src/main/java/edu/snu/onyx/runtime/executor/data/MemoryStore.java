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
package edu.snu.onyx.runtime.executor.data;

import edu.snu.onyx.runtime.exception.PartitionWriteException;
import edu.snu.onyx.runtime.executor.data.partition.MemoryPartition;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Store data in local memory.
 */
@ThreadSafe
public final class MemoryStore implements PartitionStore {
  public static final String SIMPLE_NAME = "MemoryStore";
  // A map between partition id and data blocks.
  private final ConcurrentHashMap<String, MemoryPartition> partitionMap;

  @Inject
  private MemoryStore() {
    this.partitionMap = new ConcurrentHashMap<>();
  }

  @Override
  public void createPartition(final String partitionId) {
    partitionMap.put(partitionId, new MemoryPartition());
  }

  /**
   * @see PartitionStore#getFromPartition(String, HashRange).
   */
  @Override
  public Optional<Iterable> getFromPartition(final String partitionId,
                                                      final HashRange hashRange) {
    final MemoryPartition partition = partitionMap.get(partitionId);

    if (partition != null) {
      final Iterable<Block> blocks = partition.getBlocks();
      // Retrieves data in the hash range from the target partition
      final List<Iterable> retrievedData = new ArrayList<>();
      blocks.forEach(block -> {
        if (hashRange.includes(block.getKey())) {
          retrievedData.add(block.getData());
        }
      });

      return Optional.of(concatBlocks(retrievedData));
    } else {
      return Optional.empty();
    }
  }

  /**
   * @see PartitionStore#putToPartition(String, Iterable, boolean).
   */
  @Override
  public Optional<List<Long>> putToPartition(final String partitionId,
                                             final Iterable<Block> blocks,
                                             final boolean commitPerBlock) throws PartitionWriteException {
    try {
      final MemoryPartition partition = partitionMap.get(partitionId);
      if (partition == null) {
        throw new PartitionWriteException(new Throwable("The partition " + partitionId + "is not created yet."));
      }
      partition.appendBlocks(blocks);

      // The partition is not serialized.
      return Optional.empty();
    } catch (final IOException e) {
      // The partition is committed already.
      throw new PartitionWriteException(new Throwable("This partition is already committed."));
    }
  }

  /**
   * @see PartitionStore#commitPartition(String).
   */
  @Override
  public void commitPartition(final String partitionId) {
    final MemoryPartition partition = partitionMap.get(partitionId);
    if (partition != null) {
      partition.commit();
    } else {
      throw new PartitionWriteException(new Throwable("There isn't any partition with id " + partitionId));
    }
  }

  /**
   * @see PartitionStore#removePartition(String).
   */
  @Override
  public Boolean removePartition(final String partitionId) {
    return partitionMap.remove(partitionId) != null;
  }

  /**
   * concatenates an iterable of blocks into a single iterable of elements.
   *
   * @param blocks the iterable of blocks to concatenate.
   * @return the concatenated iterable of all elements.
   */
  private Iterable concatBlocks(final Iterable<Iterable> blocks) {
    final List concatStreamBase = new ArrayList<>();
    Stream<Object> concatStream = concatStreamBase.stream();
    for (final Iterable block : blocks) {
      concatStream = Stream.concat(concatStream, StreamSupport.stream(block.spliterator(), false));
    }
    return concatStream.collect(Collectors.toList());
  }
}
