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

import java.util.HashSet;

/**
 * Indices of tasks that must not concurrently run on the same executor.
 */
public final class ResourceAntiAffinityProperty extends VertexExecutionProperty<HashSet<Integer>> {
  /**
   * Default constructor.
   *
   * @param value value of the ExecutionProperty
   */
  private ResourceAntiAffinityProperty(final HashSet<Integer> value) {
    super(value);
  }

  /**
   * Static method getting execution property.
   *
   * @param value value of the new execution property
   * @return the execution property
   */
  public static ResourceAntiAffinityProperty of(final HashSet<Integer> value) {
    return new ResourceAntiAffinityProperty(value);
  }
}
