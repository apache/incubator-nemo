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
package edu.snu.vortex.runtime.executor.partition;

import edu.snu.vortex.compiler.ir.Element;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Store data in local memory.
 */
@ThreadSafe
public final class LocalStore implements PartitionStore {
  private final ConcurrentHashMap<String, Partition> partitionIdToData;

  @Inject
  public LocalStore() {
    this.partitionIdToData = new ConcurrentHashMap<>();
  }

  @Override
  public Optional<Partition> getPartition(final String partitionId) {
    return Optional.ofNullable(partitionIdToData.get(partitionId));
  }

  @Override
  public Optional<Long> putPartition(final String partitionId, final Iterable<Element> data) {
    if (partitionIdToData.containsKey(partitionId)) {
      throw new RuntimeException("Trying to overwrite an existing partition");
    }

    partitionIdToData.put(partitionId, new LocalPartition(data));

    // The partition is not serialized.
    return Optional.empty();
  }

  @Override
  public Optional<Partition> removePartition(final String partitionId) {
    return Optional.ofNullable(partitionIdToData.remove(partitionId));
  }

  /**
   * This class represents a {@link Partition} which is stored in {@link LocalStore} and not divided in multiple blocks.
   */
  private final class LocalPartition implements Partition {

    private final Iterable<Element> data;

    private LocalPartition(final Iterable<Element> data) {
      this.data = data;
    }

    @Override
    public Iterable<Element> asIterable() {
      return data;
    }

    @Override
    public Optional<Long> size() {
      return Optional.empty();
    }
  }
}
