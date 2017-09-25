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

import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.exception.PartitionWriteException;
import edu.snu.vortex.runtime.executor.data.partition.MemoryPartition;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
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

  /**
   * @see PartitionStore#getBlocks(String, HashRange).
   */
  @Override
  public Optional<CompletableFuture<Iterable<Element>>> getBlocks(final String partitionId,
                                                                  final HashRange hashRange) {
    final MemoryPartition partition = partitionMap.get(partitionId);

    if (partition != null) {
      final CompletableFuture<Iterable<Element>> future = new CompletableFuture<>();
      final Iterable<Block> blocks = partition.getBlocks();
      // Retrieves data in the hash range from the target partition
      final List<Iterable<Element>> retrievedData = new ArrayList<>();
      blocks.forEach(block -> {
        if (hashRange.includes(block.getKey())) {
          retrievedData.add(block.getData());
        }
      });

      future.complete(concatBlocks(retrievedData));
      return Optional.of(future);
    } else {
      return Optional.empty();
    }
  }

  /**
   * @see PartitionStore#putBlocks(String, Iterable, boolean).
   */
  @Override
  public CompletableFuture<Optional<List<Long>>> putBlocks(final String partitionId,
                                                           final Iterable<Block> blocks,
                                                           final boolean commitPerBlock) {
    partitionMap.putIfAbsent(partitionId, new MemoryPartition());
    final CompletableFuture<Optional<List<Long>>> future = new CompletableFuture<>();
    try {
      partitionMap.get(partitionId).appendBlocks(blocks);
      // The partition is not serialized.
      future.complete(Optional.empty());
    } catch (final IOException e) {
      // The partition is committed already.
      future.completeExceptionally(new PartitionWriteException(new Throwable("This partition is already committed.")));
    }

    return future;
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
  public CompletableFuture<Boolean> removePartition(final String partitionId) {
    return CompletableFuture.completedFuture(partitionMap.remove(partitionId) != null);
  }

  /**
   * concatenates an iterable of blocks into a single iterable of elements.
   *
   * @param blocks the iterable of blocks to concatenate.
   * @return the concatenated iterable of all elements.
   */
  private Iterable<Element> concatBlocks(final Iterable<Iterable<Element>> blocks) {
    final List<Element> concatStreamBase = new ArrayList<>();
    Stream<Element> concatStream = concatStreamBase.stream();
    for (final Iterable<Element> block : blocks) {
      concatStream = Stream.concat(concatStream, StreamSupport.stream(block.spliterator(), false));
    }
    return concatStream.collect(Collectors.toList());
  }
}
