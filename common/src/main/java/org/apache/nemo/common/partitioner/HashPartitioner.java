/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.common.partitioner;

import org.apache.nemo.common.KeyExtractor;

/**
 * An implementation of {@link Partitioner} which hashes output data from a source task
 * according to the key of elements.
 * The data will be hashed by their key, and applied "modulo" operation by the number of partitions.
 */
public final class HashPartitioner implements Partitioner<Integer> {
  private final int numOfPartitions;
  private final KeyExtractor keyExtractor;

  /**
   * Constructor.
   *
   * @param numOfPartitions the number of partitions.
   * @param keyExtractor    the key extractor that extracts keys from elements.
   */
  public HashPartitioner(final int numOfPartitions,
                         final KeyExtractor keyExtractor) {
    this.keyExtractor = keyExtractor;
    this.numOfPartitions = numOfPartitions;
  }

  @Override
  public Integer partition(final Object element) {
    return Math.abs(keyExtractor.extractKey(element).hashCode() % numOfPartitions);
  }

  public Integer partition(final Object element, final int numOfSubPartitions) {
    return Math.abs(keyExtractor.extractKey(element).hashCode() % numOfSubPartitions);
  }
}
