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
package edu.snu.onyx.runtime.executor.data;

import edu.snu.onyx.runtime.exception.PartitionFetchException;

/**
 * Interface for partition placement which can spill some stale data.
 */
public interface SpillablePartitionStore extends PartitionStore {
  /**
   * Gets data in a partition as a form of {@link Block} to spill to another {@link PartitionStore}.
   *
   * @param partitionId of the target partition.
   * @return the result data from the target partition.
   * @throws PartitionFetchException for any error occurred while trying to fetch a partition.
   *         (This exception will be thrown to the {@link edu.snu.onyx.runtime.master.scheduler.Scheduler}
   *          through {@link edu.snu.onyx.runtime.executor.Executor} and
   *          have to be handled by the scheduler with fault tolerance mechanism.)
   */
  Iterable<Block> getBlocksFromPartition(String partitionId) throws PartitionFetchException;
}
