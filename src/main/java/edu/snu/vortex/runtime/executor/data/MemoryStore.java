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

import edu.snu.vortex.common.Pair;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.executor.data.partition.MemoryPartition;
import edu.snu.vortex.runtime.executor.data.partition.Partition;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
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
final class MemoryStore implements PartitionStore {
  // A map between partition id and data.
  private final ConcurrentHashMap<String, Iterable<Element>> partitionIdToData;
  // A map between partition id and pair hash value and data block.
  private final ConcurrentHashMap<String, Iterable<Pair<Integer, Iterable<Element>>>> partitionDataInBlocks;

  @Inject
  private MemoryStore() {
    this.partitionIdToData = new ConcurrentHashMap<>();
    this.partitionDataInBlocks = new ConcurrentHashMap<>();
  }

  /**
   * @see PartitionStore#retrieveDataFromPartition(String).
   */
  @Override
  public CompletableFuture<Optional<Partition>> retrieveDataFromPartition(final String partitionId) {
    final Iterable<Element> partitionData = partitionIdToData.get(partitionId);
    final Iterable<Pair<Integer, Iterable<Element>>> blockInfo = partitionDataInBlocks.get(partitionId);

    final Optional<Partition> partitionOptional;
    if (partitionData != null) {
      partitionOptional = Optional.of(new MemoryPartition(partitionData));
    } else if (blockInfo != null) {
      final List<Iterable<Element>> blocks = new LinkedList<>();
      blockInfo.forEach(pair -> blocks.add(pair.right()));
      partitionOptional = Optional.of(new MemoryPartition(concatBlocks(blocks)));
    } else {
      partitionOptional = Optional.empty();
    }
    return CompletableFuture.completedFuture(partitionOptional);
  }

  /**
   * @see PartitionStore#retrieveDataFromPartition(String, HashRange).
   */
  @Override
  public CompletableFuture<Optional<Partition>> retrieveDataFromPartition(final String partitionId,
                                                                          final HashRange hashRange) {
    final CompletableFuture<Optional<Partition>> future = new CompletableFuture<>();
    final Iterable<Pair<Integer, Iterable<Element>>> blockInfo = partitionDataInBlocks.get(partitionId);

    if (blockInfo != null) {
      // Retrieves data in the hash range from the target partition
      final List<Iterable<Element>> retrievedData = new ArrayList<>(hashRange.length());
      final Iterator<Pair<Integer, Iterable<Element>>> iterator = blockInfo.iterator();
      iterator.forEachRemaining(pair -> {
        if (hashRange.includes(pair.left())) {
          retrievedData.add(pair.right());
        } else {
          iterator.next();
        }
      });

      if (!future.isCompletedExceptionally()) {
        future.complete(Optional.of(new MemoryPartition(concatBlocks(retrievedData))));
      }
    } else {
      future.complete(Optional.empty());
    }
    return future;
  }

  /**
   * @see PartitionStore#putDataAsPartition(String, Iterable).
   */
  @Override
  public CompletableFuture<Optional<Long>> putDataAsPartition(final String partitionId,
                                                              final Iterable<Element> data) {
    final Iterable<Element> previousData = partitionIdToData.putIfAbsent(partitionId, data);
    if (previousData != null) {
      throw new RuntimeException("Trying to overwrite an existing partition");
    }

    partitionIdToData.put(partitionId, data);

    // The partition is not serialized.
    return CompletableFuture.completedFuture(Optional.empty());
  }

  /**
   * @see PartitionStore#putHashedDataAsPartition(String, Iterable).
   */
  @Override
  public CompletableFuture<Optional<List<Long>>> putHashedDataAsPartition(
      final String partitionId,
      final Iterable<Pair<Integer, Iterable<Element>>> hashedData) {
    final Iterable<Pair<Integer, Iterable<Element>>> previousBlockedData =
        partitionDataInBlocks.putIfAbsent(partitionId, hashedData);
    if (previousBlockedData != null) {
      throw new RuntimeException("Trying to overwrite an existing partition");
    }

    partitionDataInBlocks.put(partitionId, hashedData);

    // The partition is not serialized.
    return CompletableFuture.completedFuture(Optional.empty());
  }

  /**
   * @see PartitionStore#removePartition(String).
   */
  @Override
  public CompletableFuture<Boolean> removePartition(final String partitionId) {
    return CompletableFuture.completedFuture(partitionIdToData.remove(partitionId) != null
        || partitionDataInBlocks.remove(partitionId) != null);
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
