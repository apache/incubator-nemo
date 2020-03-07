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
 * This property decides the hash partition set of sampled & optimized tasks in Dynamic Task Sizing Policy.
 * <p>
 * Adopting this property requires changing other properties as well.
 * Such execution properties include:
 * {@link org.apache.nemo.common.ir.edge.executionproperty.PartitionerProperty}
 * {@link org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty}
 * </p>
 * Changing order matters: one must need to change PartitionerProperty, SubPartitionSetProperty,
 * and ParallelismProperty sequentially.
 */
public class SubPartitionSetProperty extends EdgeExecutionProperty<ArrayList<KeyRange>> {
  /**
   * Default constructor.
   *
   * @param value value of the EdgeExecutionProperty.
   */
  public SubPartitionSetProperty(final ArrayList<KeyRange> value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static SubPartitionSetProperty of(final ArrayList<KeyRange> value) {
    return new SubPartitionSetProperty(value);
  }
}
