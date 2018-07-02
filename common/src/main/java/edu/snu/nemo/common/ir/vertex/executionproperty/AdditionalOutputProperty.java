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

import java.util.HashMap;

/**
 * TaggedOutput Execution Property for vertex that outputs additional BEAM tagged outputs.
 */
public final class AdditionalOutputProperty extends VertexExecutionProperty<HashMap<String, String>> {
  /**
   * Constructor.
   * @param value map of tag to IRVertex id.
   */
  private AdditionalOutputProperty(final HashMap<String, String> value) {
    super(value);
  }

  /**
   * Static method exposing constructor.
   * @param value map of tag to IRVertex id.
   * @return the newly created execution property.
   */
  public static AdditionalOutputProperty of(final HashMap<String, String> value) {
    return new AdditionalOutputProperty(value);
  }
}
