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

/**
 * A collection of data elements. The data is stored as an array of bytes.
 * This is a unit of read / write towards {@link edu.snu.onyx.runtime.executor.data.block.Block}s.
 * @param <K> the key type of its partitions.
 */
public final class SerializedPartition<K> implements Partition<byte[], K> {
  private final K key;
  private final long elementsTotal;
  private final byte[] serializedData;
  private final int length;

  /**
   * Creates a serialized {@link Partition} having a specific key value.
   *
   * @param key            the key.
   * @param elementsTotal  the total number of elements.
   * @param serializedData the serialized data.
   * @param length         the length of the actual serialized data. (It can be different with serializedData.length)
   */
  public SerializedPartition(final K key,
                             final long elementsTotal,
                             final byte[] serializedData,
                             final int length) {
    this.key = key;
    this.elementsTotal = elementsTotal;
    this.serializedData = serializedData;
    this.length = length;
  }

  /**
   * @return the key value.
   */
  @Override
  public K getKey() {
    return key;
  }

  /**
   * @return whether the data in this {@link Partition} is serialized or not.
   */
  @Override
  public boolean isSerialized() {
    return true;
  }

  /**
   * @return the serialized data.
   */
  @Override
  public byte[] getData() {
    return serializedData;
  }

  /**
   * @return the length of the actual data.
   */
  public int getLength() {
    return length;
  }

  /**
   * @return the number of elements.
   */
  public long getElementsTotal() {
    return elementsTotal;
  }
}
