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
import edu.snu.nemo.runtime.executor.data.NonSerializedPartition;
import edu.snu.nemo.runtime.executor.data.SerializedPartition;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * This interface represents a block, which is the output of a specific task.
 * @param <K> the key type of its partitions.
 */
public interface Block<K extends Serializable> {

  /**
   * Stores {@link NonSerializedPartition}s to this block.
   * Invariant: This should not be invoked after this block is committed.
   *
   * @param partitions the {@link NonSerializedPartition}s to store.
   * @return the size of the data per partition (only when the data is serialized in this method).
   * @throws IOException if fail to store.
   */
  Optional<List<Long>> putPartitions(final Iterable<NonSerializedPartition<K>> partitions) throws IOException;

  /**
   * Stores {@link SerializedPartition}s to this block.
   * Invariant: This should not be invoked after this block is committed.
   *
   * @param partitions the {@link SerializedPartition}s to store.
   * @return the size of the data per partition.
   * @throws IOException if fail to store.
   */
  List<Long> putSerializedPartitions(final Iterable<SerializedPartition<K>> partitions) throws IOException;

  /**
   * Retrieves the {@link NonSerializedPartition}s in a specific key range from this block.
   * If the data is serialized, deserializes it.
   * Invariant: This should not be invoked before this block is committed.
   *
   * @param keyRange the key range to retrieve.
   * @return an iterable of {@link NonSerializedPartition}s.
   * @throws IOException if failed to retrieve.
   */
  Iterable<NonSerializedPartition<K>> getPartitions(final KeyRange<K> keyRange) throws IOException;

  /**
   * Retrieves the {@link SerializedPartition}s in a specific key range.
   * Invariant: This should not be invoked before this block is committed.
   *
   * @param keyRange the hash range to retrieve.
   * @return an iterable of {@link SerializedPartition}s.
   * @throws IOException if failed to retrieve.
   */
  Iterable<SerializedPartition<K>> getSerializedPartitions(final KeyRange<K> keyRange) throws IOException;

  /**
   * Commits this block to prevent further write.
   *
   * @throws IOException if failed to commit.
   */
  void commit() throws IOException;
}
