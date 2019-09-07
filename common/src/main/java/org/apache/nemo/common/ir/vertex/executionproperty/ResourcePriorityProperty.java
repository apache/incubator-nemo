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
 * ExecutionPlacement ExecutionProperty.
 */
public final class ResourcePriorityProperty extends VertexExecutionProperty<String> {
  /**
   * Constructor.
   *
   * @param value value of the execution property.
   */
  private ResourcePriorityProperty(final String value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static ResourcePriorityProperty of(final String value) {
    return new ResourcePriorityProperty(value);
  }

  // List of default pre-configured values.
  public static final String NONE = "None";
  public static final String TRANSIENT = "Transient";
  public static final String RESERVED = "Reserved";
  public static final String COMPUTE = "Compute";
}
