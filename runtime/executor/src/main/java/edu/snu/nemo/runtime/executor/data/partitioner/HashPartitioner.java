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
package edu.snu.nemo.runtime.executor.data.partitioner;

import edu.snu.nemo.common.KeyExtractor;

/**
 * An implementation of {@link Partitioner} which hashes output data from a source task
 * according to the key of elements.
 * The data will be hashed by their key, and applied "modulo" operation by the number of destination tasks.
 */
public final class HashPartitioner implements Partitioner<Integer> {
  private final KeyExtractor keyExtractor;
  private final int dstParallelism;

  /**
   * Constructor.
   *
   * @param dstParallelism the number of destination tasks.
   * @param keyExtractor   the key extractor that extracts keys from elements.
   */
  public HashPartitioner(final int dstParallelism,
                         final KeyExtractor keyExtractor) {
    this.keyExtractor = keyExtractor;
    this.dstParallelism = dstParallelism;
  }

  @Override
  public Integer partition(final Object element) {
    return Math.abs(keyExtractor.extractKey(element).hashCode() % dstParallelism);
  }
}
