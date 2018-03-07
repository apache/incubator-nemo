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
package edu.snu.nemo.runtime.executor.data.stores;

import edu.snu.nemo.common.exception.BlockFetchException;
import edu.snu.nemo.common.exception.BlockWriteException;
import edu.snu.nemo.runtime.common.data.KeyRange;
import edu.snu.nemo.runtime.executor.data.partition.NonSerializedPartition;
import edu.snu.nemo.runtime.executor.data.partition.SerializedPartition;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

/**
 * Interface for {@link edu.snu.nemo.runtime.executor.data.block.Block} placement.
 */
public interface BlockStore {
  /**
   * Creates a new block.
   * A stale data created by previous failed task should be handled during the creation of new block.
   *
   * @param blockId the ID of the block to create.
   * @throws BlockWriteException for any error occurred while trying to create a block.
   *                             (This exception will be thrown to the scheduler
   *                             through {@link edu.snu.nemo.runtime.executor.Executor} and
   *                             have to be handled by the scheduler with fault tolerance mechanism.)
   */
  void createBlock(String blockId) throws BlockWriteException;

  /**
   * Writes an element to non-committed block.
   * Invariant: This method may not support concurrent write for a single block.
   * Only one thread have to write at once.
   *
   * @param blockId the ID of the block to write element.
   * @param key     the key.
   * @param element the element to write.
   * @param <K>     the key type of the partitions.
   * @throws BlockWriteException for any error occurred while trying to write a block.
   *                             (This exception will be thrown to the scheduler
   *                             through {@link edu.snu.nemo.runtime.executor.Executor} and
   *                             have to be handled by the scheduler with fault tolerance mechanism.)
   */
  <K extends Serializable> void write(String blockId,
                                      K key,
                                      Object element) throws BlockWriteException;

  /**
   * Writes an iterable of {@link NonSerializedPartition}s to a block.
   * If the block exists already, appends the data to it.
   * Invariant: This method may not support concurrent write for a single block.
   * Only one thread have to write at once.
   *
   * @param blockId    of the block.
   * @param partitions to save to a block.
   * @param <K>        the key type of the partitions.
   * @throws BlockWriteException for any error occurred while trying to write a block.
   *                             (This exception will be thrown to the scheduler
   *                             through {@link edu.snu.nemo.runtime.executor.Executor} and
   *                             have to be handled by the scheduler with fault tolerance mechanism.)
   */
  <K extends Serializable> void writePartitions(String blockId,
                                                Iterable<NonSerializedPartition<K>> partitions)
      throws BlockWriteException;

  /**
   * Writes an iterable of {@link SerializedPartition}s to a block.
   * If the block exists already, appends the data to it.
   * Invariant: This method may not support concurrent write for a single block.
   * Only one thread have to write at once.
   *
   * @param blockId    of the block.
   * @param partitions to save to a block.
   * @param <K>        the key type of the partitions.
   * @throws BlockWriteException for any error occurred while trying to write a block.
   *                             (This exception will be thrown to the scheduler
   *                             through {@link edu.snu.nemo.runtime.executor.Executor} and
   *                             have to be handled by the scheduler with fault tolerance mechanism.)
   */
  <K extends Serializable> void writeSerializedPartitions(String blockId,
                                                          Iterable<SerializedPartition<K>> partitions)
      throws BlockWriteException;

  /**
   * Retrieves {@link NonSerializedPartition}s.
   * They belong to a specific {@link edu.snu.nemo.runtime.common.data.KeyRange} from a block.
   *
   * @param blockId  of the target partition.
   * @param keyRange the key range.
   * @param <K>      the key type of the partitions.
   * @return the result elements from the target block (if the target block exists).
   * @throws BlockFetchException for any error occurred while trying to fetch a block.
   *                             (This exception will be thrown to the scheduler
   *                             through {@link edu.snu.nemo.runtime.executor.Executor} and
   *                             have to be handled by the scheduler with fault tolerance mechanism.)
   */
  <K extends Serializable> Optional<Iterable<NonSerializedPartition<K>>> readPartitions(String blockId,
                                                                                        KeyRange<K> keyRange)
      throws BlockFetchException;

  /**
   * Retrieves {@link SerializedPartition}s in a specific {@link KeyRange} from a block.
   *
   * @param blockId  of the target block.
   * @param keyRange the key range.
   * @param <K>      the key type of the partitions.
   * @return the result elements from the target block (if the target block exists).
   * @throws BlockFetchException for any error occurred while trying to fetch a partition.
   *                             (This exception will be thrown to the scheduler
   *                             through {@link edu.snu.nemo.runtime.executor.Executor} and
   *                             have to be handled by the scheduler with fault tolerance mechanism.)
   */
  <K extends Serializable> Optional<Iterable<SerializedPartition<K>>> readSerializedPartitions(String blockId,
                                                                                               KeyRange<K> keyRange)
      throws BlockFetchException;

  /**
   * Notifies that all writes for a block is end.
   *
   * @param blockId of the block.
   * @param <K>     the key type of the partitions.
   * @return the size of each partition if the data in the block is serialized.
   * @throws BlockWriteException if fail to commit.
   *                             (This exception will be thrown to the scheduler
   *                             through {@link edu.snu.nemo.runtime.executor.Executor} and
   *                             have to be handled by the scheduler with fault tolerance mechanism.)
   */
  <K extends Serializable> Optional<Map<K, Long>> commitBlock(String blockId) throws BlockWriteException;

  /**
   * Removes a block of data.
   *
   * @param blockId of the block.
   * @return whether the partition exists or not.
   * @throws BlockFetchException for any error occurred while trying to remove a block.
   *                             (This exception will be thrown to the scheduler
   *                             through {@link edu.snu.nemo.runtime.executor.Executor} and
   *                             have to be handled by the scheduler with fault tolerance mechanism.)
   */
  boolean removeBlock(String blockId);
}
