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
 * This is a unit of read / write towards {@link edu.snu.onyx.runtime.executor.data.partition.Partition}s.
 */
public final class SerializedBlock implements Block<byte[]> {
  private final int key;
  private final long elementsTotal;
  private final byte[] serializedData;

  /**
   * Creates a serialized {@link Block} having a specific key value.
   *
   * @param key            the key.
   * @param elementsTotal  the total number of elements.
   * @param serializedData the serialized data.
   */
  public SerializedBlock(final int key,
                         final long elementsTotal,
                         final byte[] serializedData) {
    this.key = key;
    this.elementsTotal = elementsTotal;
    this.serializedData = serializedData;
  }

  /**
   * @return the key value.
   */
  @Override
  public int getKey() {
    return key;
  }

  /**
   * @return whether the data in this {@link Block} is serialized or not.
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
   * @return the number of elements.
   */
  public long getElementsTotal() {
    return elementsTotal;
  }
}
