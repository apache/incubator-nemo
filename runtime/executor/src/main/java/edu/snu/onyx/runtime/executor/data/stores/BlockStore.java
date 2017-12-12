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
package edu.snu.onyx.runtime.executor.data.stores;

import edu.snu.onyx.common.exception.BlockFetchException;
import edu.snu.onyx.common.exception.BlockWriteException;
import edu.snu.onyx.runtime.common.data.HashRange;
import edu.snu.onyx.runtime.executor.data.NonSerializedPartition;
import edu.snu.onyx.runtime.executor.data.SerializedPartition;

import java.util.List;
import java.util.Optional;

/**
 * Interface for {@link edu.snu.onyx.runtime.executor.data.block.Block} placement.
 */
public interface BlockStore {
  /**
   * Creates a new block.
   * A stale data created by previous failed task should be handled during the creation of new block.
   *
   * @param blockId the ID of the block to create.
   * @throws BlockWriteException for any error occurred while trying to create a block.
   *         (This exception will be thrown to the scheduler
   *          through {@link edu.snu.onyx.runtime.executor.Executor} and
   *          have to be handled by the scheduler with fault tolerance mechanism.)
   */
  void createBlock(String blockId) throws BlockWriteException;

  /**
   * Saves an iterable of {@link NonSerializedPartition}s to a block.
   * If the block exists already, appends the data to it.
   * Invariant: This method may not support concurrent write for a single block.
   *            Only one thread have to write at once.
   *
   * @param blockId            of the block.
   * @param partitions         to save to a block.
   * @param commitPerPartition whether commit every partition write or not.
   * @return the size of the data per partition (only when the data is serialized).
   * @throws BlockWriteException for any error occurred while trying to write a block.
   *         (This exception will be thrown to the scheduler
   *          through {@link edu.snu.onyx.runtime.executor.Executor} and
   *          have to be handled by the scheduler with fault tolerance mechanism.)
   */
  Optional<List<Long>> putPartitions(String blockId,
                                     Iterable<NonSerializedPartition> partitions,
                                     boolean commitPerPartition) throws BlockWriteException;

  /**
   * Saves an iterable of {@link SerializedPartition}s to a block.
   * If the block exists already, appends the data to it.
   * Invariant: This method may not support concurrent write for a single block.
   *            Only one thread have to write at once.
   *
   * @param blockId            of the block.
   * @param partitions         to save to a block.
   * @param commitPerPartition whether commit every partition write or not.
   * @return the size of the data per partition (only when the data is serialized).
   * @throws BlockWriteException for any error occurred while trying to write a block.
   *         (This exception will be thrown to the scheduler
   *          through {@link edu.snu.onyx.runtime.executor.Executor} and
   *          have to be handled by the scheduler with fault tolerance mechanism.)
   */
  List<Long> putSerializedPartitions(String blockId,
                                     Iterable<SerializedPartition> partitions,
                                     boolean commitPerPartition) throws BlockWriteException;

  /**
   * Retrieves {@link NonSerializedPartition}s in a specific {@link HashRange} from a block.
   *
   * @param blockId   of the target partition.
   * @param hashRange the hash range.
   * @return the result elements from the target block (if the target block exists).
   * @throws BlockFetchException for any error occurred while trying to fetch a block.
   *         (This exception will be thrown to the scheduler
   *          through {@link edu.snu.onyx.runtime.executor.Executor} and
   *          have to be handled by the scheduler with fault tolerance mechanism.)
   */
  Optional<Iterable<NonSerializedPartition>> getPartitions(String blockId,
                                                           HashRange hashRange) throws BlockFetchException;

  /**
   * Retrieves {@link SerializedPartition}s in a specific {@link HashRange} from a block.
   *
   * @param blockId   of the target block.
   * @param hashRange the hash range.
   * @return the result elements from the target block (if the target block exists).
   * @throws BlockFetchException for any error occurred while trying to fetch a partition.
   *         (This exception will be thrown to the scheduler
   *          through {@link edu.snu.onyx.runtime.executor.Executor} and
   *          have to be handled by the scheduler with fault tolerance mechanism.)
   */
  Optional<Iterable<SerializedPartition>> getSerializedPartitions(String blockId,
                                                                  HashRange hashRange) throws BlockFetchException;

  /**
   * Notifies that all writes for a block is end.
   * Subscribers waiting for the data of the target block are notified when the block is committed.
   * Also, further subscription about a committed block will not blocked but get the data in it and finished.
   *
   * @param blockId of the block.
   * @throws BlockWriteException if fail to commit.
   *         (This exception will be thrown to the scheduler
   *          through {@link edu.snu.onyx.runtime.executor.Executor} and
   *          have to be handled by the scheduler with fault tolerance mechanism.)
   */
  void commitBlock(String blockId) throws BlockWriteException;

  /**
   * Removes a block of data.
   *
   * @param blockId of the block.
   * @return whether the partition exists or not.
   * @throws BlockFetchException for any error occurred while trying to remove a block.
   *         (This exception will be thrown to the scheduler
   *          through {@link edu.snu.onyx.runtime.executor.Executor} and
   *          have to be handled by the scheduler with fault tolerance mechanism.)
   */
  Boolean removeBlock(String blockId);
}
