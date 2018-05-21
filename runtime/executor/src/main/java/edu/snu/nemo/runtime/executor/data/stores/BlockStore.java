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
import edu.snu.nemo.runtime.executor.data.block.Block;

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
   * @return the created block.
   * @throws BlockWriteException for any error occurred while trying to create a block.
   *                             (This exception will be thrown to the scheduler
   *                             through {@link edu.snu.nemo.runtime.executor.Executor} and
   *                             have to be handled by the scheduler with fault tolerance mechanism.)
   */
  Block createBlock(String blockId) throws BlockWriteException;

  /**
   * Writes a committed block to this store.
   *
   * @param block the block to write.
   * @throws BlockWriteException if fail to write.
   *                             (This exception will be thrown to the scheduler
   *                             through {@link edu.snu.nemo.runtime.executor.Executor} and
   *                             have to be handled by the scheduler with fault tolerance mechanism.)
   */
  void writeBlock(Block block) throws BlockWriteException;

  /**
   * Reads a committed block from this store.
   *
   * @param blockId of the target partition.
   * @return the target block (if it exists).
   * @throws BlockFetchException for any error occurred while trying to fetch a block.
   *                             (This exception will be thrown to the scheduler
   *                             through {@link edu.snu.nemo.runtime.executor.Executor} and
   *                             have to be handled by the scheduler with fault tolerance mechanism.)
   */
  Optional<Block> readBlock(String blockId) throws BlockFetchException;

  /**
   * Deletes a block from this store.
   *
   * @param blockId of the block.
   * @return whether the partition exists or not.
   * @throws BlockFetchException for any error occurred while trying to remove a block.
   *                             (This exception will be thrown to the scheduler
   *                             through {@link edu.snu.nemo.runtime.executor.Executor} and
   *                             have to be handled by the scheduler with fault tolerance mechanism.)
   */
  boolean deleteBlock(String blockId);
}
