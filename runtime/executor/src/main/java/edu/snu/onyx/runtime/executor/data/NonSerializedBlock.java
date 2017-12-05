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
 * A collection of data elements. The data is stored as an iterable of elements.
 * This is a unit of read / write towards {@link edu.snu.onyx.runtime.executor.data.partition.Partition}s.
 */
public final class NonSerializedBlock implements Block<Iterable> {
  private final int key;
  private final Iterable nonSerializedData;

  /**
   * Creates a non-serialized {@link Block} having a specific key value.
   *
   * @param key  the key.
   * @param data the non-serialized data.
   */
  public NonSerializedBlock(final int key,
                            final Iterable data) {
    this.key = key;
    this.nonSerializedData = data;
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
    return false;
  }

  /**
   * @return the non-serialized data.
   */
  @Override
  public Iterable getData() {
    return nonSerializedData;
  }
}
