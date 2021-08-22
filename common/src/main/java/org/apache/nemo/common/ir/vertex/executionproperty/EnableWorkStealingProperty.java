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

/**
 * Marks Work Stealing Strategy of the vertex.
 *
 * Currently, there are three types:
 * SPLIT  : vertex which is the subject of work stealing
 * MERGE  : vertex which merges the effect of work stealing
 * DEFAULT : vertex which is not the subject of work stealing
 */
public class EnableWorkStealingProperty extends VertexExecutionProperty<String> {
  /**
   * Default constructor.
   *
   * @param value value of the VertexExecutionProperty.
   */
  public EnableWorkStealingProperty(final String value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static EnableWorkStealingProperty of(final String value) {
    return new EnableWorkStealingProperty(value);
  }
}
