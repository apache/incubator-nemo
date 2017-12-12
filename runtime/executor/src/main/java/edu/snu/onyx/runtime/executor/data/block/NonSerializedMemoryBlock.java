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
import edu.snu.onyx.runtime.executor.data.DataUtil;
import edu.snu.onyx.runtime.common.data.HashRange;
import edu.snu.onyx.runtime.executor.data.NonSerializedPartition;
import edu.snu.onyx.runtime.executor.data.SerializedPartition;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * This class represents a block which is stored in local memory and not serialized.
 */
@ThreadSafe
public final class NonSerializedMemoryBlock implements Block {

  private final List<NonSerializedPartition> nonSerializedPartitions;
  private final Coder coder;
  private volatile boolean committed;

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
  public synchronized Optional<List<Long>> putPartitions(final Iterable<NonSerializedPartition> partitions)
      throws IOException {
    if (!committed) {
      partitions.forEach(nonSerializedPartitions::add);
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
  public synchronized List<Long> putSerializedPartitions(final Iterable<SerializedPartition> partitions)
      throws IOException {
    if (!committed) {
      final Iterable<NonSerializedPartition> convertedPartitions =
          DataUtil.convertToNonSerPartitions(coder, partitions);
      final List<Long> dataSizePerPartition = new ArrayList<>();
      partitions.forEach(serializedPartition -> dataSizePerPartition.add((long) serializedPartition.getData().length));
      putPartitions(convertedPartitions);

      return dataSizePerPartition;
    } else {
      throw new IOException("Cannot append partitions to the committed block");
    }
  }

  /**
   * Retrieves the {@link NonSerializedPartition}s in a specific hash range from this block.
   * Invariant: This should not be invoked before this block is committed.
   *
   * @param hashRange the hash range to retrieve.
   * @return an iterable of {@link NonSerializedPartition}s.
   * @throws IOException if failed to retrieve.
   */
  @Override
  public Iterable<NonSerializedPartition> getPartitions(final HashRange hashRange) throws IOException {
    if (committed) {
      // Retrieves data in the hash range from the target block
      final List<NonSerializedPartition> retrievedPartitions = new ArrayList<>();
      nonSerializedPartitions.forEach(partition -> {
        final int key = partition.getKey();
        if (hashRange.includes(key)) {
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
   * @param hashRange the hash range to retrieve.
   * @return an iterable of {@link SerializedPartition}s.
   * @throws IOException if failed to retrieve.
   */
  @Override
  public Iterable<SerializedPartition> getSerializedPartitions(final HashRange hashRange) throws IOException {
    return DataUtil.convertToSerPartitions(coder, getPartitions(hashRange));
  }

  /**
   * Commits this block to prevent further write.
   */
  @Override
  public synchronized void commit() {
    committed = true;
  }
}
