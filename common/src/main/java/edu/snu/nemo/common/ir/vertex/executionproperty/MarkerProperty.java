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
 * A property represents that a vertex annotated with this property should be regarded as a kind of "marker" and
 * doesn't process any data.
 * Attaching this property makes runtime to not schedule this vertex.
 * This kind of vertices is needed when some data have to be written before it's usage is not determined yet
 * (e.g., for caching).
 */
public final class MarkerProperty extends VertexExecutionProperty<Boolean> {

  private static final MarkerProperty MARKER_PROPERTY = new MarkerProperty();

  /**
   * Constructor.
   */
  private MarkerProperty() {
    super(true);
  }

  /**
   * Static method exposing the constructor.
   *
   * @return the execution property.
   */
  public static MarkerProperty of() {
    return MARKER_PROPERTY;
  }
}
