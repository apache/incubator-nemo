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

package org.apache.nemo.common.ir.vertex.executionproperty;

import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;

import java.util.ArrayList;
import java.util.Set;

/**
 * List of set of node names to limit the scheduling of the tasks of the vertex to while shuffling.
 * For example, [[1, 2, 3], [4, 5, 6]] limits shuffle to occur just within nodes 1, 2, 3 and nodes 4, 5, 6 separately.
 * This occurs by limiting the source executors where the tasks read their input data from, depending on
 * where the task is located at.
 * ShuffleExecutorSetProperty is set only for the IntermediateAccumulatorVertex, and
 * other vertices should not have this property.
 */
public final class ShuffleExecutorSetProperty extends VertexExecutionProperty<ArrayList<Set<String>>> {

  /**
   * Default constructor.
   * @param value value of the execution property.
   */
  private ShuffleExecutorSetProperty(final ArrayList<Set<String>> value) {
    super(value);
  }

  /**
   * Static method for constructing {@link ShuffleExecutorSetProperty}.
   *
   * @param setsOfExecutors the list of executors to schedule the tasks of the vertex on.
   *                        Leave empty to make it effectless.
   * @return the new execution property
   */
  public static ShuffleExecutorSetProperty of(final Set<Set<String>> setsOfExecutors) {
    return new ShuffleExecutorSetProperty(new ArrayList<>(setsOfExecutors));
  }
}
