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
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for remote partition stores (e.g., GlusterFS, ...).
 */
@DefaultImplementation(GlusterFileStore.class)
interface RemoteFileStore extends PartitionStore {

  /**
   * Concurrently appends an iterable of data blocks to a partition.
   * Each block has a specific hash value, and the block becomes a unit of read & write.
   * However, the blocks may not be saved consecutively.
   *
   * @param partitionId of the partition.
   * @param hashedData  to save . Each pair consists of the hash value and the block data.
   * @return the size of data per hash value.
   *         (the future completes exceptionally with {@link edu.snu.vortex.runtime.exception.PartitionWriteException}
   *          for any error occurred while trying to write a partition.)
   */
  CompletableFuture<List<Long>> appendHashedData(String partitionId,
                                                 Iterable<Pair<Integer, Iterable<Element>>> hashedData);
}
