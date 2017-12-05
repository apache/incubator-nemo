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
import edu.snu.onyx.runtime.executor.data.PartitionManagerWorker;
import edu.snu.onyx.runtime.executor.data.partition.SerializedMemoryPartition;
import org.apache.reef.tang.InjectionFuture;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

/**
 * Serialize and store data in local memory.
 */
@ThreadSafe
public final class SerializedMemoryStore extends LocalPartitionStore {

  @Inject
  private SerializedMemoryStore(final InjectionFuture<PartitionManagerWorker> partitionManagerWorker) {
    super(partitionManagerWorker);
  }

  @Override
  public void createPartition(final String partitionId) {
    final Coder coder = getCoderFromWorker(partitionId);
    getPartitionMap().put(partitionId, new SerializedMemoryPartition(coder));
  }

  /**
   * @see PartitionStore#removePartition(String).
   */
  @Override
  public Boolean removePartition(final String partitionId) {
    return getPartitionMap().remove(partitionId) != null;
  }
}
