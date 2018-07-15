/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.nemo.runtime.executor.data.block;

import edu.snu.nemo.common.exception.BlockFetchException;
import edu.snu.nemo.common.exception.BlockWriteException;
import edu.snu.nemo.common.KeyRange;
import edu.snu.nemo.runtime.executor.data.DataUtil;
import edu.snu.nemo.runtime.executor.data.partition.NonSerializedPartition;
import edu.snu.nemo.runtime.executor.data.partition.SerializedPartition;
import edu.snu.nemo.runtime.executor.data.streamchainer.Serializer;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * This class represents a block which is serialized and stored in local memory.
 * Concurrent read is supported, but concurrent write is not supported.
 *
 * @param <K> the key type of its partitions.
 */
@NotThreadSafe
public final class SerializedMemoryBlock<K extends Serializable> implements Block<K> {

  private final String id;
  private final List<SerializedPartition<K>> serializedPartitions;
  private final Map<K, SerializedPartition<K>> nonCommittedPartitionsMap;
  private final Serializer serializer;
  private volatile boolean committed;

  /**
   * Constructor.
   *
   * @param blockId    the ID of this block.
   * @param serializer the {@link Serializer}.
   */
  public SerializedMemoryBlock(final String blockId,
                               final Serializer serializer) {
    this.id = blockId;
    this.serializedPartitions = new ArrayList<>();
    this.nonCommittedPartitionsMap = new HashMap<>();
    this.serializer = serializer;
    this.committed = false;
  }

  /**
   * Writes an element to non-committed block.
   * Invariant: This should not be invoked after this block is committed.
   * Invariant: This method does not support concurrent write.
   *
   * @param key     the key.
   * @param element the element to write.
   * @throws BlockWriteException for any error occurred while trying to write a block.
   */
  @Override
  public void write(final K key,
                    final Object element) throws BlockWriteException {
    if (committed) {
      throw new BlockWriteException(new Throwable("The partition is already committed!"));
    } else {
      try {
        SerializedPartition<K> partition = nonCommittedPartitionsMap.get(key);
        if (partition == null) {
          partition = new SerializedPartition<>(key, serializer);
          nonCommittedPartitionsMap.put(key, partition);
        }
        partition.write(element);
      } catch (final IOException e) {
        throw new BlockWriteException(e);
      }
    }
  }

  /**
   * Serialized and stores {@link NonSerializedPartition}s to this block.
   * Invariant: This should not be invoked after this block is committed.
   * Invariant: This method does not support concurrent write.
   *
   * @param partitions the {@link NonSerializedPartition}s to store.
   * @throws BlockWriteException for any error occurred while trying to write a block.
   */
  @Override
  public void writePartitions(final Iterable<NonSerializedPartition<K>> partitions) throws BlockWriteException {
    if (!committed) {
      try {
        final Iterable<SerializedPartition<K>> convertedPartitions = DataUtil.convertToSerPartitions(
            serializer, partitions);
        writeSerializedPartitions(convertedPartitions);
      } catch (final IOException e) {
        throw new BlockWriteException(e);
      }
    } else {
      throw new BlockWriteException(new Throwable("Cannot append partitions to the committed block"));
    }
  }

  /**
   * Stores {@link SerializedPartition}s to this block.
   * Invariant: This should not be invoked after this block is committed.
   * Invariant: This method does not support concurrent write.
   *
   * @param partitions the {@link SerializedPartition}s to store.
   * @throws BlockWriteException for any error occurred while trying to write a block.
   */
  @Override
  public void writeSerializedPartitions(final Iterable<SerializedPartition<K>> partitions) throws BlockWriteException {
    if (!committed) {
      partitions.forEach(serializedPartitions::add);
    } else {
      throw new BlockWriteException(new Throwable("Cannot append partitions to the committed block"));
    }
  }

  /**
   * Retrieves the {@link NonSerializedPartition}s in a specific hash range from this block.
   * Because the data is stored in a serialized form, it have to be deserialized.
   * Invariant: This should not be invoked before this block is committed.
   *
   * @param keyRange the key range to retrieve.
   * @return an iterable of {@link NonSerializedPartition}s.
   * @throws BlockFetchException for any error occurred while trying to fetch a block.
   */
  @Override
  public Iterable<NonSerializedPartition<K>> readPartitions(final KeyRange keyRange) throws BlockFetchException {
    try {
      return DataUtil.convertToNonSerPartitions(serializer, readSerializedPartitions(keyRange));
    } catch (final IOException e) {
      throw new BlockFetchException(e);
    }
  }

  /**
   * Retrieves the {@link SerializedPartition}s in a specific hash range.
   * Invariant: This should not be invoked before this block is committed.
   *
   * @param keyRange the key range to retrieve.
   * @return an iterable of {@link SerializedPartition}s.
   * @throws BlockFetchException for any error occurred while trying to fetch a block.
   */
  @Override
  public Iterable<SerializedPartition<K>> readSerializedPartitions(final KeyRange keyRange) throws BlockFetchException {
    if (committed) {
      final List<SerializedPartition<K>> partitionsInRange = new ArrayList<>();
      serializedPartitions.forEach(serializedPartition -> {
        final K key = serializedPartition.getKey();
        if (keyRange.includes(key)) {
          // The hash value of this partition is in the range.
          partitionsInRange.add(serializedPartition);
        }
      });

      return partitionsInRange;
    } else {
      throw new BlockFetchException(new Throwable("Cannot retrieve elements before a block is committed"));
    }
  }

  /**
   * Commits this block to prevent further write.
   *
   * @return the size of each partition.
   * @throws BlockWriteException for any error occurred while trying to write a block.
   */
  @Override
  public synchronized Optional<Map<K, Long>> commit() throws BlockWriteException {
    try {
      if (!committed) {
        for (final SerializedPartition<K> partition : nonCommittedPartitionsMap.values()) {
          partition.commit();
          serializedPartitions.add(partition);
        }
        nonCommittedPartitionsMap.clear();
        committed = true;
      }
      final Map<K, Long> partitionSizes = new HashMap<>(serializedPartitions.size());
      for (final SerializedPartition<K> serializedPartition : serializedPartitions) {
        final K key = serializedPartition.getKey();
        final long partitionSize = serializedPartition.getLength();
        if (partitionSizes.containsKey(key)) {
          partitionSizes.compute(key,
              (existingKey, existingValue) -> existingValue + partitionSize);
        } else {
          partitionSizes.put(key, partitionSize);
        }
      }
      return Optional.of(partitionSizes);
    } catch (final IOException e) {
      throw new BlockWriteException(e);
    }
  }

  /**
   * @return the ID of this block.
   */
  @Override
  public String getId() {
    return id;
  }

  /**
   * @return whether this block is committed or not.
   */
  @Override
  public synchronized boolean isCommitted() {
    return committed;
  }
}
