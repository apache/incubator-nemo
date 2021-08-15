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
import java.util.HashSet;

/**
 * List of set of node names to limit the scheduling of the tasks of the vertex to while shuffling.
 */
public final class ShuffleExecutorSetProperty extends VertexExecutionProperty<ArrayList<HashSet<String>>> {

  /**
   * Default constructor.
   * @param value value of the execution property.
   */
  private ShuffleExecutorSetProperty(final ArrayList<HashSet<String>> value) {
    super(value);
  }

  /**
   * Static method for constructing {@link ShuffleExecutorSetProperty}.
   *
   * @param setsOfExecutors the list of executors to schedule the tasks of the vertex on.
   *                        Leave empty to make it effectless.
   * @return the new execution property
   */
  public static ShuffleExecutorSetProperty of(final HashSet<HashSet<String>> setsOfExecutors) {
    return new ShuffleExecutorSetProperty(new ArrayList<>(setsOfExecutors));
  }
}
