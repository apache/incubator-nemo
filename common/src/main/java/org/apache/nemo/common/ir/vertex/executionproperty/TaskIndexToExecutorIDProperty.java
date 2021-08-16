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

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;

import java.util.HashMap;
import java.util.List;

/**
 * Keep track of where the tasks are located by its executor ID.
 */
public final class TaskIndexToExecutorIDProperty
  extends VertexExecutionProperty<HashMap<Integer, List<Pair<String, String>>>> {
  /**
   * Default constructor.
   * @param taskIDToExecutorIDsMap value of the execution property.
   */
  private TaskIndexToExecutorIDProperty(final HashMap<Integer, List<Pair<String, String>>> taskIDToExecutorIDsMap) {
    super(taskIDToExecutorIDsMap);
  }

  /**
   * Static method for constructing {@link TaskIndexToExecutorIDProperty}.
   *
   * @param taskIndexToExecutorIDsMap the map indicating the executor IDs where the tasks are located on.
   * @return the new execution property
   */
  public static TaskIndexToExecutorIDProperty of(
    final HashMap<Integer, List<Pair<String, String>>> taskIndexToExecutorIDsMap) {
    return new TaskIndexToExecutorIDProperty(taskIndexToExecutorIDsMap);
  }
}
