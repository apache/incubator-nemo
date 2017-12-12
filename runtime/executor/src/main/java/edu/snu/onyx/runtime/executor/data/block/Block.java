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

import edu.snu.onyx.runtime.common.data.HashRange;
import edu.snu.onyx.runtime.executor.data.NonSerializedPartition;
import edu.snu.onyx.runtime.executor.data.SerializedPartition;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * This interface represents a block, which is the output of a specific task.
 */
public interface Block {

  /**
   * Stores {@link NonSerializedPartition}s to this block.
   * Invariant: This should not be invoked after this block is committed.
   *
   * @param partitions the {@link NonSerializedPartition}s to store.
   * @return the size of the data per partition (only when the data is serialized in this method).
   * @throws IOException if fail to store.
   */
  Optional<List<Long>> putPartitions(final Iterable<NonSerializedPartition> partitions) throws IOException;

  /**
   * Stores {@link SerializedPartition}s to this block.
   * Invariant: This should not be invoked after this block is committed.
   *
   * @param partitions the {@link SerializedPartition}s to store.
   * @return the size of the data per partition.
   * @throws IOException if fail to store.
   */
  List<Long> putSerializedPartitions(final Iterable<SerializedPartition> partitions) throws IOException;

  /**
   * Retrieves the {@link NonSerializedPartition}s in a specific hash range from this block.
   * If the data is serialized, deserializes it.
   * Invariant: This should not be invoked before this block is committed.
   *
   * @param hashRange the hash range to retrieve.
   * @return an iterable of {@link NonSerializedPartition}s.
   * @throws IOException if failed to retrieve.
   */
  Iterable<NonSerializedPartition> getPartitions(final HashRange hashRange) throws IOException;

  /**
   * Retrieves the {@link SerializedPartition}s in a specific hash range.
   * Invariant: This should not be invoked before this block is committed.
   *
   * @param hashRange the hash range to retrieve.
   * @return an iterable of {@link SerializedPartition}s.
   * @throws IOException if failed to retrieve.
   */
  Iterable<SerializedPartition> getSerializedPartitions(final HashRange hashRange) throws IOException;

  /**
   * Commits this block to prevent further write.
   */
  void commit();
}
