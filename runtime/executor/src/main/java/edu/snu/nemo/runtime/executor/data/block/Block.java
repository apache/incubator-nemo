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

import edu.snu.nemo.common.exception.BlockFetchException;
import edu.snu.nemo.common.exception.BlockWriteException;
import edu.snu.nemo.runtime.common.data.KeyRange;
import edu.snu.nemo.runtime.executor.data.partition.NonSerializedPartition;
import edu.snu.nemo.runtime.executor.data.partition.SerializedPartition;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

/**
 * This interface represents a block, which is the output of a specific task.
 *
 * @param <K> the key type of its partitions.
 */
public interface Block<K extends Serializable> {

  /**
   * Writes an element to non-committed block.
   * Invariant: This should not be invoked after this block is committed.
   * Invariant: This method may not support concurrent write.
   *
   * @param key     the key.
   * @param element the element to write.
   * @throws BlockWriteException for any error occurred while trying to write a block.
   *                             (This exception will be thrown to the scheduler
   *                             through {@link edu.snu.nemo.runtime.executor.Executor} and
   *                             have to be handled by the scheduler with fault tolerance mechanism.)
   */
  void write(K key, Object element) throws BlockWriteException;

  /**
   * Stores {@link NonSerializedPartition}s to this block.
   * Invariant: This should not be invoked after this block is committed.
   * Invariant: This method may not support concurrent write.
   *
   * @param partitions the {@link NonSerializedPartition}s to store.
   * @throws BlockWriteException for any error occurred while trying to write a block.
   *                             (This exception will be thrown to the scheduler
   *                             through {@link edu.snu.nemo.runtime.executor.Executor} and
   *                             have to be handled by the scheduler with fault tolerance mechanism.)
   */
  void writePartitions(Iterable<NonSerializedPartition<K>> partitions) throws BlockWriteException;

  /**
   * Stores {@link SerializedPartition}s to this block.
   * Invariant: This should not be invoked after this block is committed.
   * Invariant: This method may not support concurrent write.
   *
   * @param partitions the {@link SerializedPartition}s to store.
   * @throws BlockWriteException for any error occurred while trying to write a block.
   *                             (This exception will be thrown to the scheduler
   *                             through {@link edu.snu.nemo.runtime.executor.Executor} and
   *                             have to be handled by the scheduler with fault tolerance mechanism.)
   */
  void writeSerializedPartitions(Iterable<SerializedPartition<K>> partitions) throws BlockWriteException;

  /**
   * Retrieves the {@link NonSerializedPartition}s in a specific key range from this block.
   * If the data is serialized, deserializes it.
   * Invariant: This should not be invoked before this block is committed.
   *
   * @param keyRange the key range to retrieve.
   * @return an iterable of {@link NonSerializedPartition}s.
   * @throws BlockFetchException for any error occurred while trying to fetch a block.
   *                             (This exception will be thrown to the scheduler
   *                             through {@link edu.snu.nemo.runtime.executor.Executor} and
   *                             have to be handled by the scheduler with fault tolerance mechanism.)
   */
  Iterable<NonSerializedPartition<K>> readPartitions(KeyRange<K> keyRange) throws BlockFetchException;

  /**
   * Retrieves the {@link SerializedPartition}s in a specific key range.
   * Invariant: This should not be invoked before this block is committed.
   *
   * @param keyRange the hash range to retrieve.
   * @return an iterable of {@link SerializedPartition}s.
   * @throws BlockFetchException for any error occurred while trying to fetch a block.
   *                             (This exception will be thrown to the scheduler
   *                             through {@link edu.snu.nemo.runtime.executor.Executor} and
   *                             have to be handled by the scheduler with fault tolerance mechanism.)
   */
  Iterable<SerializedPartition<K>> readSerializedPartitions(KeyRange<K> keyRange) throws BlockFetchException;

  /**
   * Commits this block to prevent further write.
   *
   * @return the size of each partition if the data in the block is serialized.
   * @throws BlockWriteException for any error occurred while trying to write a block.
   *                             (This exception will be thrown to the scheduler
   *                             through {@link edu.snu.nemo.runtime.executor.Executor} and
   *                             have to be handled by the scheduler with fault tolerance mechanism.)
   */
  Optional<Map<K, Long>> commit() throws BlockWriteException;

  /**
   * @return the ID of this block.
   */
  String getId();

  /**
   * @return whether this block is committed or not.
   */
  boolean isCommitted();
}
