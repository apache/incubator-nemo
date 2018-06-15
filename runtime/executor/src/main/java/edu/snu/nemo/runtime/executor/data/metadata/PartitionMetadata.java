/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.nemo.runtime.executor.data.metadata;

import java.io.Serializable;

/**
 * This class represents a metadata for a partition.
 * @param <K> the key type of its partitions.
 */
public final class PartitionMetadata<K extends Serializable> {
  private final K key;
  private final int partitionSize;
  private final long offset;
  private final long elementsTotal;

  /**
   * Constructor.
   *
   * @param key           the key of this partition.
   * @param partitionSize the size of this partition.
   * @param offset        the offset of this partition.
   * @param elementsTotal the total number of elements in this partition.
   */
  public PartitionMetadata(final K key,
                           final int partitionSize,
                           final long offset,
                           final long elementsTotal) {
    this.key = key;
    this.partitionSize = partitionSize;
    this.offset = offset;
    this.elementsTotal = elementsTotal;
  }

  /**
   * @return the key of this partition.
   */
  public K getKey() {
    return key;
  }

  /**
   * @return the size of this partition.
   */
  public int getPartitionSize() {
    return partitionSize;
  }

  /**
   * @return the offset of this partition.
   */
  public long getOffset() {
    return offset;
  }

  /**
   * @return the total number of elements in this partition.
   */
  public long getElementsTotal() {
    return elementsTotal;
  }
}
