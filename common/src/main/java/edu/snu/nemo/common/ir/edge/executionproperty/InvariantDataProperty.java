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

import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;

/**
 * Invariant data ExecutionProperty. Use to indicate same data edge when unrolling loop vertex.
 */
public final class InvariantDataProperty extends ExecutionProperty<Pair<Integer, String>> {
  /**
   * Constructor.
   * @param value value of the execution property.
   */
  private InvariantDataProperty(final Pair<Integer, String> value) {
    super(Key.InvariantData, value);
  }

  /**
   * Static method exposing the constructor.
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static InvariantDataProperty of(final Pair<Integer, String> value) {
    return new InvariantDataProperty(value);
  }
}
