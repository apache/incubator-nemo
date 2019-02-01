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

import org.apache.nemo.common.KeyRange;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

import java.util.ArrayList;

/**
 * This property decides which partitions the tasks of the destination IRVertex should fetch.
 * The position of a KeyRange in the list corresponds to the offset of the destination task.
 *
 * For example, in the following setup:
 * Source IRVertex (Parallelism=2) - IREdge (Partitioner.Num=4) - Destination IRVertex (Parallelism=2)
 *
 * Setting PartitionSetProperty([0, 3), [3, 3)) on the IREdge with will enforce the following behaviors.
 * - The first destination task fetches the first 3 partitions from each of the 2 data blocks
 * - The second destination task fetches the last partitions from each of the 2 data blocks
 *
 * This property is useful for handling data skews.
 * For example, if the size ratio of the 4 partitions in the above setup are (17%, 16%, 17%, 50%),
 * then each of the destination task will evenly handle 50% of the load.
 */
public final class PartitionSetProperty extends EdgeExecutionProperty<ArrayList<KeyRange>> {
  /**
   * Constructor.
   *
   * @param value value of the execution property.
   */
  private PartitionSetProperty(final ArrayList<KeyRange> value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static PartitionSetProperty of(final ArrayList<KeyRange> value) {
    return new PartitionSetProperty(value);
  }
}
