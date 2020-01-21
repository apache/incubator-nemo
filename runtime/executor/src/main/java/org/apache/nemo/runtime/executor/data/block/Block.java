/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.executor.data.block;

import org.apache.nemo.common.KeyRange;
import org.apache.nemo.runtime.executor.data.partition.NonSerializedPartition;
import org.apache.nemo.runtime.executor.data.partition.SerializedPartition;

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
   * Invariant: This method does not support concurrent write.
   *
   * @param key     the key.
   * @param element the element to write.
   *
   * Classes implementing this interface may throw
   * org.apache.nemo.common.exception.BlockWriteException for any error occurred while trying to write a block.
   * (This exception will be thrown to the scheduler
   * through {@link org.apache.nemo.runtime.executor.Executor} and
   * have to be handled by the scheduler with fault tolerance mechanism.)
   */
  void write(K key, Object element);

  /**
   * Stores {@link NonSerializedPartition}s to this block.
   * Invariant: This should not be invoked after this block is committed.
   * Invariant: This method does not support concurrent write.
   *
   * @param partitions the {@link NonSerializedPartition}s to store.
   *
   * Classes implementing this interface may throw
   * org.apache.nemo.common.exception.BlockWriteException for any error occurred while trying to write a block.
   * (This exception will be thrown to the scheduler
   * through {@link org.apache.nemo.runtime.executor.Executor} and
   * have to be handled by the scheduler with fault tolerance mechanism.)
   */
  void writePartitions(Iterable<NonSerializedPartition<K>> partitions);

  /**
   * Stores {@link SerializedPartition}s to this block.
   * Invariant: This should not be invoked after this block is committed.
   * Invariant: This method does not support concurrent write.
   *
   * @param partitions the {@link SerializedPartition}s to store.
   *
   * Classes implementing this interface may throw
   * org.apache.nemo.common.exception.BlockWriteException for any error occurred while trying to write a block.
   * (This exception will be thrown to the scheduler
   * through {@link org.apache.nemo.runtime.executor.Executor} and
   * have to be handled by the scheduler with fault tolerance mechanism.)
   */
  void writeSerializedPartitions(Iterable<SerializedPartition<K>> partitions);

  /**
   * Retrieves the {@link NonSerializedPartition}s in a specific key range from this block.
   * If the data is serialized, deserializes it.
   * Invariant: This should not be invoked before this block is committed.
   *
   * @param keyRange the key range to retrieve.
   * @return an iterable of {@link NonSerializedPartition}s.
   *
   * Classes implementing this interface may throw
   * org.apache.nemo.common.exception.BlockFetchException for any error occurred while trying to fetch a block.
   * (This exception will be thrown to the scheduler
   * through {@link org.apache.nemo.runtime.executor.Executor} and
   * have to be handled by the scheduler with fault tolerance mechanism.)
   */
  Iterable<NonSerializedPartition<K>> readPartitions(KeyRange<K> keyRange);

  /**
   * Retrieves the {@link SerializedPartition}s in a specific key range.
   * Invariant: This should not be invoked before this block is committed.
   *
   * @param keyRange the hash range to retrieve.
   * @return an iterable of {@link SerializedPartition}s.
   *
   * Classes implementing this interface may throw
   * org.apache.nemo.common.exception.BlockFetchException for any error occurred while trying to fetch a block.
   * (This exception will be thrown to the scheduler
   * through {@link org.apache.nemo.runtime.executor.Executor} and
   * have to be handled by the scheduler with fault tolerance mechanism.)
   */
  Iterable<SerializedPartition<K>> readSerializedPartitions(KeyRange<K> keyRange);

  /**
   * Commits this block to prevent further write.
   *
   * @return the size of each partition if the data in the block is serialized.
   *
   * Classes implementing this interface may throw
   * org.apache.nemo.common.exception.BlockWriteException for any error occurred while trying to commit a block.
   * (This exception will be thrown to the scheduler
   * through {@link org.apache.nemo.runtime.executor.Executor} and
   * have to be handled by the scheduler with fault tolerance mechanism.)
   */
  Optional<Map<K, Long>> commit();

  /**
   * Commits all un-committed partitions.
   * This method can be useful if partitions in a block should be committed before the block is committed totally.
   * For example, non-committed partitions in a file block can be flushed to storage from memory.
   * If another element is written after this method is called, a new non-committed partition should be created
   * for the element even if a partition with the same key is committed already.
   *
   * Classes implementing this interface may throw
   * org.apache.nemo.common.exception.BlockWriteException for any error occurred
   * while trying to commit partitions.
   * (This exception will be thrown to the scheduler
   * through {@link org.apache.nemo.runtime.executor.Executor} and
   * have to be handled by the scheduler with fault tolerance mechanism.)
   */
  void commitPartitions();

  /**
   * @return the ID of this block.
   */
  String getId();

  /**
   * @return whether this block is committed or not.
   */
  boolean isCommitted();
}
