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
package edu.snu.onyx.runtime.executor.data.metadata;

/**
 * This class represents a metadata for a partition.
 * @param <K> the key type of its partitions.
 */
public final class PartitionMetadata<K> {
  private final int partitionIdx;
  private final K key;
  private final int partitionSize;
  private final long offset;
  private final long elementsTotal;
  private volatile boolean committed;

  public PartitionMetadata(final int partitionIdx,
                           final K key,
                           final int partitionSize,
                           final long offset,
                           final long elementsTotal) {
    this.partitionIdx = partitionIdx;
    this.key = key;
    this.partitionSize = partitionSize;
    this.offset = offset;
    this.elementsTotal = elementsTotal;
    this.committed = false;
  }

  boolean isCommitted() {
    return committed;
  }

  void setCommitted() {
    this.committed = true;
  }

  int getPartitionIdx() {
    return partitionIdx;
  }

  public K getKey() {
    return key;
  }

  public int getPartitionSize() {
    return partitionSize;
  }

  public long getOffset() {
    return offset;
  }

  public long getElementsTotal() {
    return elementsTotal;
  }
}
