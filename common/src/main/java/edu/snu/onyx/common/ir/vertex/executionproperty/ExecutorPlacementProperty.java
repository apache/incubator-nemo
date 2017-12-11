/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.onyx.common.ir.vertex.executionproperty;

import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;

/**
 * ExecutionPlacement ExecutionProperty.
 */
public final class ExecutorPlacementProperty extends ExecutionProperty<String> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private ExecutorPlacementProperty(final String value) {
    super(Key.ExecutorPlacement, value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static ExecutorPlacementProperty of(final String value) {
    return new ExecutorPlacementProperty(value);
  }

  // List of default pre-configured values. TODO #479: Remove static values.
  public static final String NONE = "None";
  public static final String TRANSIENT = "Transient";
  public static final String RESERVED = "Reserved";
  public static final String COMPUTE = "Compute";
}
