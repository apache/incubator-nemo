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
package edu.snu.onyx.runtime.executor.data.partition;

import edu.snu.onyx.runtime.common.data.HashRange;
import edu.snu.onyx.runtime.executor.data.NonSerializedBlock;
import edu.snu.onyx.runtime.executor.data.SerializedBlock;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * This interface represents a partition, which is the output of a specific task.
 */
public interface Partition {

  /**
   * Stores {@link NonSerializedBlock}s to this partition.
   * Invariant: This should not be invoked after this partition is committed.
   *
   * @param blocks the {@link NonSerializedBlock}s to store.
   * @return the size of the data per block (only when the data is serialized in this method).
   * @throws IOException if fail to store.
   */
  Optional<List<Long>> putBlocks(final Iterable<NonSerializedBlock> blocks) throws IOException;

  /**
   * Stores {@link SerializedBlock}s to this partition.
   * Invariant: This should not be invoked after this partition is committed.
   *
   * @param blocks the {@link SerializedBlock}s to store.
   * @return the size of the data per block.
   * @throws IOException if fail to store.
   */
  List<Long> putSerializedBlocks(final Iterable<SerializedBlock> blocks) throws IOException;

  /**
   * Retrieves the {@link NonSerializedBlock}s in a specific hash range from this partition.
   * If the data is serialized, deserializes it.
   * Invariant: This should not be invoked before this partition is committed.
   *
   * @param hashRange the hash range to retrieve.
   * @return an iterable of {@link NonSerializedBlock}s.
   * @throws IOException if failed to retrieve.
   */
  Iterable<NonSerializedBlock> getBlocks(final HashRange hashRange) throws IOException;

  /**
   * Retrieves the {@link SerializedBlock}s in a specific hash range.
   * Invariant: This should not be invoked before this partition is committed.
   *
   * @param hashRange the hash range to retrieve.
   * @return an iterable of {@link SerializedBlock}s.
   * @throws IOException if failed to retrieve.
   */
  Iterable<SerializedBlock> getSerializedBlocks(final HashRange hashRange) throws IOException;

  /**
   * Commits this partition to prevent further write.
   */
  void commit();
}
