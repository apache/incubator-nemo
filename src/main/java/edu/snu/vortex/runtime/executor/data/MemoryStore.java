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
import edu.snu.vortex.runtime.executor.data.partition.LocalPartition;
import edu.snu.vortex.runtime.executor.data.partition.Partition;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Store data in local memory.
 */
@ThreadSafe
final class MemoryStore implements PartitionStore {
  private final ConcurrentHashMap<String, Partition> partitionIdToData;

  @Inject
  private MemoryStore() {
    this.partitionIdToData = new ConcurrentHashMap<>();
  }

  @Override
  public Optional<Partition> getPartition(final String partitionId) {
    return Optional.ofNullable(partitionIdToData.get(partitionId));
  }

  @Override
  public Optional<Long> putPartition(final String partitionId, final Iterable<Element> data) {
    final Partition previousPartition = partitionIdToData.putIfAbsent(partitionId, new LocalPartition(data));
    if (previousPartition != null) {
      throw new RuntimeException("Trying to overwrite an existing partition");
    }

    partitionIdToData.put(partitionId, new LocalPartition(data));

    // The partition is not serialized.
    return Optional.empty();
  }

  @Override
  public boolean removePartition(final String partitionId) {
    return partitionIdToData.remove(partitionId) != null;
  }
}
