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
package edu.snu.onyx.common.ir.edge.executionproperty;

import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;

/**
 * UsedDataHandling ExecutionProperty.
 * This property represents the used data handling strategy.
 */
public final class UsedDataHandlingProperty extends ExecutionProperty<UsedDataHandlingProperty.Value> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private UsedDataHandlingProperty(final Value value) {
    super(Key.UsedDataHandling, value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static UsedDataHandlingProperty of(final Value value) {
    return new UsedDataHandlingProperty(value);
  }

  /**
   * Possible values of UsedDataHandling ExecutionProperty.
   */
  public enum Value {
    Discard,
    Keep
  }
}
