/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.nemo.common.ir.edge.executionproperty;

import edu.snu.nemo.common.ir.executionproperty.EdgeExecutionProperty;

/**
 * Decompression ExecutionProperty.
 * It shares the value with {@link CompressionProperty}.
 */
public final class DecompressionProperty extends EdgeExecutionProperty<CompressionProperty.Value> {
  /**
   * Constructor.
   *
   * @param value value of the execution property.
   */
  private DecompressionProperty(final CompressionProperty.Value value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static DecompressionProperty of(final CompressionProperty.Value value) {
    return new DecompressionProperty(value);
  }
}
