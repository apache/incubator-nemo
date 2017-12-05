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

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.runtime.common.RuntimeIdGenerator;
import edu.snu.onyx.runtime.executor.data.PartitionManagerWorker;
import org.apache.reef.tang.InjectionFuture;

/**
 * This abstract class represents a default {@link PartitionStore},
 * which contains other components used in each implementation of {@link PartitionStore}.
 */
public abstract class AbstractPartitionStore implements PartitionStore {
  private final InjectionFuture<PartitionManagerWorker> partitionManagerWorker;

  protected AbstractPartitionStore(final InjectionFuture<PartitionManagerWorker> partitionManagerWorker) {
    this.partitionManagerWorker = partitionManagerWorker;
  }

  /**
   * Gets data coder for a partition from the {@link PartitionManagerWorker}.
   *
   * @param partitionId the ID of the partition to get the coder.
   * @return the coder.
   */
  public final Coder getCoderFromWorker(final String partitionId) {
    final String runtimeEdgeId = RuntimeIdGenerator.getRuntimeEdgeIdFromPartitionId(partitionId);
    return partitionManagerWorker.get().getCoder(runtimeEdgeId);
  }
}
