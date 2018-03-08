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
package edu.snu.nemo.runtime.executor.data.block;

import edu.snu.nemo.runtime.common.data.KeyRange;
import edu.snu.nemo.runtime.executor.data.DataUtil;
import edu.snu.nemo.runtime.executor.data.partition.NonSerializedPartition;
import edu.snu.nemo.runtime.executor.data.partition.SerializedPartition;
import edu.snu.nemo.runtime.executor.data.streamchainer.Serializer;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * This class represents a block which is stored in local memory and not serialized.
 * Concurrent read is supported, but concurrent write is not supported.
 *
 * @param <K> the key type of its partitions.
 */
@NotThreadSafe
public final class NonSerializedMemoryBlock<K extends Serializable> implements Block<K> {

  private final List<NonSerializedPartition<K>> nonSerializedPartitions;
  private final Map<K, NonSerializedPartition<K>> nonCommittedPartitionsMap;
  private final Serializer serializer;
  private volatile boolean committed;

  /**
   * Constructor.
   *
   * @param serializer the {@link Serializer}.
   */
  public NonSerializedMemoryBlock(final Serializer serializer) {
    this.nonSerializedPartitions = new ArrayList<>();
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
   * @throws IOException if this block is already committed.
   */
  @Override
  public void write(final K key,
                    final Object element) throws IOException {
    if (committed) {
      throw new IOException("The partition is already committed!");
    } else {
      final NonSerializedPartition<K> partition =
          nonCommittedPartitionsMap.computeIfAbsent(key, absentKey -> new NonSerializedPartition<>(key));
      partition.write(element);
    }
  }

  /**
   * Stores {@link NonSerializedPartition}s to this block.
   * Invariant: This should not be invoked after this block is committed.
   * Invariant: This method does not support concurrent write.
   *
   * @param partitions the {@link NonSerializedPartition}s to store.
   * @throws IOException if fail to store.
   */
  @Override
  public void writePartitions(final Iterable<NonSerializedPartition<K>> partitions) throws IOException {
    if (!committed) {
      partitions.forEach(nonSerializedPartitions::add);
    } else {
      throw new IOException("Cannot append partition to the committed block");
    }
  }

  /**
   * Stores {@link SerializedPartition}s to this block.
   * Because all data in this block is stored in a non-serialized form,
   * the data in these partitions have to be deserialized.
   * Invariant: This should not be invoked after this block is committed.
   * Invariant: This method does not support concurrent write.
   *
   * @param partitions the {@link SerializedPartition}s to store.
   * @throws IOException if fail to store.
   */
  @Override
  public void writeSerializedPartitions(final Iterable<SerializedPartition<K>> partitions) throws IOException {
    if (!committed) {
      final Iterable<NonSerializedPartition<K>> convertedPartitions =
          DataUtil.convertToNonSerPartitions(serializer, partitions);
      writePartitions(convertedPartitions);
    } else {
      throw new IOException("Cannot append partitions to the committed block");
    }
  }

  /**
   * Retrieves the {@link NonSerializedPartition}s in a specific hash range from this block.
   * Invariant: This should not be invoked before this block is committed.
   *
   * @param keyRange the hash range to retrieve.
   * @return an iterable of {@link NonSerializedPartition}s.
   * @throws IOException if failed to retrieve.
   */
  @Override
  public Iterable<NonSerializedPartition<K>> readPartitions(final KeyRange keyRange) throws IOException {
    if (committed) {
      // Retrieves data in the hash range from the target block
      final List<NonSerializedPartition<K>> retrievedPartitions = new ArrayList<>();
      nonSerializedPartitions.forEach(partition -> {
        final K key = partition.getKey();
        if (keyRange.includes(key)) {
          retrievedPartitions.add(partition);
        }
      });

      return retrievedPartitions;
    } else {
      throw new IOException("Cannot retrieve elements before a block is committed");
    }
  }

  /**
   * Retrieves the {@link SerializedPartition}s in a specific hash range.
   * Because the data is stored in a non-serialized form, it have to be serialized.
   * Invariant: This should not be invoked before this block is committed.
   *
   * @param keyRange the key range to retrieve.
   * @return an iterable of {@link SerializedPartition}s.
   * @throws IOException if failed to retrieve.
   */
  @Override
  public Iterable<SerializedPartition<K>> readSerializedPartitions(final KeyRange keyRange) throws IOException {
    return DataUtil.convertToSerPartitions(serializer, readPartitions(keyRange));
  }

  /**
   * Commits this block to prevent further write.
   *
   * @return empty optional because the data is not serialized.
   */
  @Override
  public synchronized Optional<Map<K, Long>> commit() {
    if (!committed) {
      nonCommittedPartitionsMap.forEach((key, partition) -> {
        partition.commit();
        nonSerializedPartitions.add(partition);
      });
      nonCommittedPartitionsMap.clear();
      committed = true;
    }
    return Optional.empty();
  }
}
