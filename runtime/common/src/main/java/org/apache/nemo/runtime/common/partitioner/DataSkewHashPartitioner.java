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
package org.apache.nemo.runtime.common.partitioner;

import org.apache.nemo.common.KeyExtractor;
import org.apache.nemo.runtime.common.optimizer.pass.runtime.DataSkewRuntimePass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link Partitioner} which hashes output data from a source task appropriate to detect data skew.
 * It hashes data finer than {@link HashPartitioner}.
 * The elements will be hashed by their key, and applied "modulo" operation.
 *
 * When we need to split or recombine the output data from a task after it is stored,
 * we multiply the hash range with a multiplier, which is commonly-known by the source and destination tasks,
 * to prevent the extra deserialize - rehash - serialize process.
 * For more information, please check {@link DataSkewRuntimePass#HASH_RANGE_MULTIPLIER}.
 */
public final class DataSkewHashPartitioner implements Partitioner<Integer> {
  private static final Logger LOG = LoggerFactory.getLogger(DataSkewHashPartitioner.class.getName());
  private final KeyExtractor keyExtractor;
  private final int hashRange;

  /**
   * Constructor.
   *
   * @param dstParallelism      the number of destination tasks.
   * @param keyExtractor        the key extractor that extracts keys from elements.
   */
  public DataSkewHashPartitioner(final int dstParallelism,
                                 final KeyExtractor keyExtractor) {
    this.keyExtractor = keyExtractor;
    // For this hash range, please check the description of HashRangeMultiplier in JobConf.
    this.hashRange = dstParallelism * DataSkewRuntimePass.HASH_RANGE_MULTIPLIER;
  }

  @Override
  public Integer partition(final Object element) {
    return Math.abs(keyExtractor.extractKey(element).hashCode() % hashRange);
  }
}
