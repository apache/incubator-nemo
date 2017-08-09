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

import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.exception.PartitionFetchException;
import edu.snu.vortex.runtime.exception.PartitionWriteException;
import edu.snu.vortex.runtime.executor.data.partition.Partition;

import java.util.List;
import java.util.Optional;

/**
 * Interface for partition placement.
 */
public interface PartitionStore {
  /**
   * Retrieves whole data from a partition.
   * @param partitionId of the partition.
   * @return the partition if exist, or an empty optional else.
   * @throws PartitionFetchException thrown if the partition is exist but fail to get the partition.
   */
  Optional<Partition> retrieveDataFromPartition(String partitionId) throws PartitionFetchException;

  /**
   * Retrieves data in a specific hash range from a partition.
   * The result data will be treated as another partition.
   * @param partitionId of the target partition.
   * @param hashRangeStartVal of the hash range (included in the range).
   * @param hashRangeEndVal of the hash range (excluded from the range).
   * @return the result data as a new partition (if the target partition exists).
   * @throws PartitionFetchException thrown for any error occurred while trying to fetch a partition
   */
  Optional<Partition> retrieveDataFromPartition(String partitionId,
                                                int hashRangeStartVal,
                                                int hashRangeEndVal) throws PartitionFetchException;

  /**
   * Saves data as a partition.
   * @param partitionId of the partition.
   * @param data of to save as a partition.
   * @return the size of the data (only when the data is serialized).
   * @throws PartitionWriteException thrown for any error occurred while trying to write a partition
   */
  Optional<Long> putDataAsPartition(String partitionId,
                                    Iterable<Element> data) throws PartitionWriteException;

  /**
   * Saves an iterable of data blocks as a partition.
   * Each block has a specific hash value, and these blocks are sorted by this hash value.
   * The block becomes a unit of read & write.
   * @param partitionId of the partition.
   * @param sortedData to save as a partition.
   * @return the size of data per hash value (only when the data is serialized).
   * @throws PartitionWriteException thrown for any error occurred while trying to write a partition
   */
  Optional<List<Long>> putSortedDataAsPartition(String partitionId,
                                                Iterable<Iterable<Element>> sortedData)
      throws PartitionWriteException;

  /**
   * Optional<Partition> removePartition(String partitionId) throws PartitionFetchException;
   * Removes a partition of data.
   * @param partitionId of the partition.
   * @return whether the partition exists or not.
   * @throws PartitionFetchException thrown for any error occurred while trying to remove a partition
   */
  boolean removePartition(String partitionId) throws PartitionFetchException;
}
