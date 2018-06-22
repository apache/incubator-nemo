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
package edu.snu.nemo.common.ir.vertex.executionproperty;

import edu.snu.nemo.common.ir.executionproperty.VertexExecutionProperty;

/**
 * Attaching this property makes runtime to skip serialization and deserialization for the vertex input and output.
 * TODO #118: Implement Skipping (De)Serialization by ExecutionProperty
 */
public final class SkipSerDesProperty extends VertexExecutionProperty<Boolean> {

  private static final SkipSerDesProperty SKIP_SER_DES_PROPERTY = new SkipSerDesProperty();

  /**
   * Constructor.
   */
  private SkipSerDesProperty() {
    super(true);
  }

  /**
   * Static method exposing the constructor.
   * @return the execution property.
   */
  public static SkipSerDesProperty of() {
    return SKIP_SER_DES_PROPERTY;
  }
}
