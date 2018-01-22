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
package edu.snu.coral.runtime.executor.data.block;

import edu.snu.coral.runtime.common.data.KeyRange;
import edu.snu.coral.runtime.executor.data.DataUtil;
import edu.snu.coral.runtime.executor.data.NonSerializedPartition;
import edu.snu.coral.runtime.executor.data.SerializedPartition;
import edu.snu.coral.runtime.executor.data.chainable.Serializer;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * This class represents a block which is serialized and stored in local memory.
 * @param <K> the key type of its partitions.
 */
@ThreadSafe
public final class SerializedMemoryBlock<K extends Serializable> implements Block<K> {

  private final List<SerializedPartition<K>> serializedPartitions;
  private final Serializer serializer;
  private volatile boolean committed;

  /**
   * Constructor.
   *
   * @param serializer the {@link Serializer}.
   */
  public SerializedMemoryBlock(final Serializer serializer) {
    this.serializer = serializer;
    serializedPartitions = new ArrayList<>();
    committed = false;
  }

  /**
   * Serialized and stores {@link NonSerializedPartition}s to this block.
   * Invariant: This should not be invoked after this block is committed.
   *
   * @param partitions the {@link NonSerializedPartition}s to store.
   * @return the size of the data per partition.
   * @throws IOException if fail to store.
   */
  @Override
  public synchronized Optional<List<Long>> putPartitions(final Iterable<NonSerializedPartition<K>> partitions)
      throws IOException {
    if (!committed) {
      final Iterable<SerializedPartition<K>> convertedPartitions = DataUtil.convertToSerPartitions(
          serializer, partitions);
      return Optional.of(putSerializedPartitions(convertedPartitions));
    } else {
      throw new IOException("Cannot append partitions to the committed block");
    }
  }

  /**
   * Stores {@link SerializedPartition}s to this block.
   * Invariant: This should not be invoked after this block is committed.
   *
   * @param partitions the {@link SerializedPartition}s to store.
   * @throws IOException if fail to store.
   */
  @Override
  public synchronized List<Long> putSerializedPartitions(final Iterable<SerializedPartition<K>> partitions)
      throws IOException {
    if (!committed) {
      final List<Long> partitionSizeList = new ArrayList<>();
      partitions.forEach(serializedPartition -> {
        partitionSizeList.add((long) serializedPartition.getLength());
        serializedPartitions.add(serializedPartition);
      });

      return partitionSizeList;
    } else {
      throw new IOException("Cannot append partitions to the committed block");
    }
  }

  /**
   * Retrieves the {@link NonSerializedPartition}s in a specific hash range from this block.
   * Because the data is stored in a serialized form, it have to be deserialized.
   * Invariant: This should not be invoked before this block is committed.
   *
   * @param keyRange the key range to retrieve.
   * @return an iterable of {@link NonSerializedPartition}s.
   * @throws IOException if failed to retrieve.
   */
  @Override
  public Iterable<NonSerializedPartition<K>> getPartitions(final KeyRange keyRange) throws IOException {
    return DataUtil.convertToNonSerPartitions(serializer, getSerializedPartitions(keyRange));
  }

  /**
   * Retrieves the {@link SerializedPartition}s in a specific hash range.
   * Invariant: This should not be invoked before this block is committed.
   *
   * @param keyRange the key range to retrieve.
   * @return an iterable of {@link SerializedPartition}s.
   * @throws IOException if failed to retrieve.
   */
  @Override
  public Iterable<SerializedPartition<K>> getSerializedPartitions(final KeyRange keyRange) throws IOException {
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
      throw new IOException("Cannot retrieve elements before a block is committed");
    }
  }

  /**
   * Commits this block to prevent further write.
   */
  @Override
  public synchronized void commit() {
    committed = true;
  }
}
