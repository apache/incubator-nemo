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
package edu.snu.vortex.runtime.executor.data;

import edu.snu.vortex.common.Pair;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.executor.data.partition.Partition;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for partition placement.
 */
public interface PartitionStore {
  /**
   * Retrieves whole data from a partition.
   * @param partitionId of the partition.
   * @return the partition if exist, or an empty optional else.
   *         (the future completes exceptionally with {@link edu.snu.vortex.runtime.exception.PartitionFetchException}
   *          if the partition exists but it was unable to get the partition.)
   */
  CompletableFuture<Optional<Partition>> retrieveDataFromPartition(String partitionId);

  /**
   * Retrieves data in a specific hash range from a partition.
   * The result data will be treated as another partition.
   * @param partitionId of the target partition.
   * @param hashRange   the hash range
   * @return the result data as a new partition (if the target partition exists).
   *         (the future completes exceptionally with {@link edu.snu.vortex.runtime.exception.PartitionFetchException}
   *          for any error occurred while trying to fetch a partition.)
   */
  CompletableFuture<Optional<Partition>> retrieveDataFromPartition(String partitionId,
                                                                   HashRange hashRange);

  /**
   * Saves data as a partition.
   * @param partitionId of the partition.
   * @param data of to save as a partition.
   * @return the size of the data (only when the data is serialized).
   *         (the future completes with {@link edu.snu.vortex.runtime.exception.PartitionWriteException}
   *          for any error occurred while trying to write a partition.)
   */
  CompletableFuture<Optional<Long>> putDataAsPartition(String partitionId,
                                                       Iterable<Element> data);

  /**
   * Saves an iterable of data blocks as a partition.
   * Each block has a specific hash value, and the block becomes a unit of read & write.
   * @param partitionId of the partition.
   * @param hashedData to save as a partition. Each pair consists of the hash value and the block data.
   * @return the size of data per hash value (only when the data is serialized).
   *         (the future completes exceptionally with {@link edu.snu.vortex.runtime.exception.PartitionWriteException}
   *          for any error occurred while trying to write a partition.)
   */
  CompletableFuture<Optional<List<Long>>> putHashedDataAsPartition(
      String partitionId,
      Iterable<Pair<Integer, Iterable<Element>>> hashedData);

  /**
   * Optional<Partition> removePartition(String partitionId) throws PartitionFetchException;
   * Removes a partition of data.
   * @param partitionId of the partition.
   * @return whether the partition exists or not.
   *         (the future completes exceptionally with {@link edu.snu.vortex.runtime.exception.PartitionFetchException}
   *          for any error occurred while trying to remove a partition.)
   */
  CompletableFuture<Boolean> removePartition(String partitionId);
}
