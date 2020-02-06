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
package org.apache.nemo.common.ir.edge.executionproperty;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

/**
 * Partitioner ExecutionProperty.
 */
public final class PartitionerProperty
  extends EdgeExecutionProperty<Pair<PartitionerProperty.Type, Integer>> {

  // Lazily use the number of destination parallelism to minimize the number of partitions.
  public static final int NUM_EQUAL_TO_DST_PARALLELISM = 0;

  /**
   * Constructor.
   *
   * @param value value of the execution property.
   */
  private PartitionerProperty(final Pair<Type, Integer> value) {
    super(value);
  }

  /**
   * @param type of the partitioner.
   * @return the property.
   */
  public static PartitionerProperty of(final Type type) {
    return PartitionerProperty.of(type, NUM_EQUAL_TO_DST_PARALLELISM, true);
  }

  /**
   * @param type            of the partitioner.
   * @param numOfPartitions to create.
   * @return the property.
   */
  public static PartitionerProperty of(final Type type, final int numOfPartitions) {
    return PartitionerProperty.of(type, numOfPartitions, false);
  }

  /**
   * @param type            of the partitioner.
   * @param numOfPartitions to create.
   * @param auto            if the number of partitions is auto.
   * @return the property.
   */
  private static PartitionerProperty of(final Type type, final int numOfPartitions, final boolean auto) {
    if (!auto && numOfPartitions <= 0) {
      throw new IllegalArgumentException(String.valueOf(numOfPartitions));
    }
    return new PartitionerProperty(Pair.of(type, numOfPartitions));
  }

  /**
   * Static constructor.
   * This is used by reflection by the MetricUtils class.
   *
   * @param value the Pair value.
   * @return the new execution property.
   */
  public static PartitionerProperty of(final Pair<Type, Integer> value) {
    return new PartitionerProperty(value);
  }

  /**
   * Partitioning types.
   */
  public enum Type {
    HASH,
    INTACT,
    DEDICATED_KEY_PER_ELEMENT
  }
}
