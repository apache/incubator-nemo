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
package edu.snu.onyx.runtime.executor.data.block;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.runtime.common.data.KeyRange;
import edu.snu.onyx.runtime.executor.data.DataUtil;
import edu.snu.onyx.runtime.executor.data.NonSerializedPartition;
import edu.snu.onyx.runtime.executor.data.SerializedPartition;
import edu.snu.onyx.runtime.executor.data.blocktransfer.BlockOutputStream;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class represents a block which is stored in local memory and not serialized.
 * @param <K> the key type of its partitions.
 */
@ThreadSafe
public final class NonSerializedMemoryBlock<K extends Serializable> implements Block<K> {

  private final List<NonSerializedPartition<K>> nonSerializedPartitions;
  private final Coder coder;
  private volatile boolean committed;
  private final Map<BlockOutputStream<?>, KeyRange> subscriptions = new ConcurrentHashMap<>();

  public NonSerializedMemoryBlock(final Coder coder) {
    this.nonSerializedPartitions = new ArrayList<>();
    this.coder = coder;
    this.committed = false;
  }

  /**
   * Stores {@link NonSerializedPartition}s to this block.
   * Invariant: This should not be invoked after this block is committed.
   *
   * @param partitions the {@link NonSerializedPartition}s to store.
   * @throws IOException if fail to store.
   */
  @Override
  public synchronized Optional<List<Long>> putPartitions(final Iterable<NonSerializedPartition<K>> partitions)
      throws IOException {
    if (!committed) {
      partitions.forEach(nonSerializedPartitions::add);
      for (final Map.Entry<BlockOutputStream<?>, KeyRange> entry : subscriptions.entrySet()) {
        final BlockOutputStream<?> stream = entry.getKey();
        final KeyRange keyRange = entry.getValue();
        for (final NonSerializedPartition<K> partition : partitions) {
          if (keyRange.includes(partition.getKey())) {
            stream.writeElements(partition.getData());
          }
        }
      }
    } else {
      throw new IOException("Cannot append partition to the committed block");
    }

    return Optional.empty();
  }

  /**
   * Stores {@link SerializedPartition}s to this block.
   * Because all data in this block is stored in a non-serialized form,
   * the data in these partitions have to be deserialized.
   * Invariant: This should not be invoked after this block is committed.
   *
   * @param partitions the {@link SerializedPartition}s to store.
   * @throws IOException if fail to store.
   */
  @Override
  public synchronized List<Long> putSerializedPartitions(final Iterable<SerializedPartition<K>> partitions)
      throws IOException {
    if (!committed) {
      final Iterable<NonSerializedPartition<K>> convertedPartitions =
          DataUtil.convertToNonSerPartitions(coder, partitions);
      final List<Long> dataSizePerPartition = new ArrayList<>();
      partitions.forEach(serializedPartition -> dataSizePerPartition.add((long) serializedPartition.getData().length));
      putPartitions(convertedPartitions);

      for (final Map.Entry<BlockOutputStream<?>, KeyRange> entry : subscriptions.entrySet()) {
        final BlockOutputStream<?> stream = entry.getKey();
        final KeyRange keyRange = entry.getValue();
        for (final SerializedPartition<K> partition : partitions) {
          if (keyRange.includes(partition.getKey())) {
            stream.writeSerializedPartitions(Collections.singletonList(partition));
          }
        }
      }

      return dataSizePerPartition;
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
  public Iterable<NonSerializedPartition<K>> getPartitions(final KeyRange keyRange) throws IOException {
    if (committed) {
      // Retrieves data in the hash range from the target block
      final List<NonSerializedPartition<K>> retrievedPartitions = new ArrayList<>();
      nonSerializedPartitions.forEach(partition -> {
        final K key = partition.getKey();
        if (keyRange.includes(key)) {
          retrievedPartitions.add(new NonSerializedPartition(key, partition.getData()));
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
  public Iterable<SerializedPartition<K>> getSerializedPartitions(final KeyRange keyRange) throws IOException {
    return DataUtil.convertToSerPartitions(coder, getPartitions(keyRange));
  }

  /**
   * Commits this block to prevent further write.
   */
  @Override
  public synchronized void commit() {
    committed = true;
    try {
      for (final BlockOutputStream<?> stream : subscriptions.keySet()) {
        stream.close();
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Register a {@link BlockOutputStream} to specific partition request.
   * @param stream    the {@link BlockOutputStream} to write on
   * @param keyRange  key range
   */
  public synchronized void subscribe(final BlockOutputStream<?> stream, final KeyRange keyRange) {
    subscriptions.put(stream, keyRange);
  }
}
