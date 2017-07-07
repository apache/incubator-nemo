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

import java.util.Optional;

/**
 * Interface for partition placement.
 */
public interface PartitionStore {
  /**
   * Retrieves a partition of data.
   * @param partitionId of the partition.
   * @return the partition (optionally).
   * @throws PartitionFetchException thrown for any error occurred while trying to fetch a partition
   */
  Optional<Partition> getPartition(String partitionId) throws PartitionFetchException;

  /**
   * Saves a partition of data.
   * @param partitionId of the partition.
   * @param data of the partition.
   * @return the size of the data (only when the data is serialized).
   * @throws PartitionWriteException thrown for any error occurred while trying to write a partition
   */
  Optional<Long> putPartition(String partitionId, Iterable<Element> data) throws PartitionWriteException;

  /**
   * Optional<Partition> removePartition(String partitionId) throws PartitionFetchException;
   * Removes a partition of data.
   * @param partitionId of the partition.
   * @return whether the partition exists or not.
   * @throws PartitionFetchException thrown for any error occurred while trying to fetch a partition
   */
  boolean removePartition(String partitionId) throws PartitionFetchException;
}
