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

import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

import java.io.Serializable;

/**
 * Edges with this property fetch a broadcast variable.
 */
public final class BroadcastVariableIdProperty extends EdgeExecutionProperty<Serializable> {

  /**
   * Constructor.
   * @param value id.
   */
  private BroadcastVariableIdProperty(final Serializable value) {
    super(value);
  }

  /**
   * Static method exposing constructor.
   * @param value id.
   * @return the newly created execution property.
   */
  public static BroadcastVariableIdProperty of(final Serializable value) {
    return new BroadcastVariableIdProperty(value);
  }
}
