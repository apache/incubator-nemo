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
 * Attaching this property makes runtime to not schedule this ghost vertex.
 * This kind of vertices is needed when some data have to be written before it's usage is not determined yet
 * (e.g., for caching).
 */
public final class GhostProperty extends VertexExecutionProperty<Boolean> {

  private static final GhostProperty GHOST_PROPERTY = new GhostProperty();

  /**
   * Constructor.
   */
  private GhostProperty() {
    super(true);
  }

  /**
   * Static method exposing the constructor.
   *
   * @return the execution property.
   */
  public static GhostProperty of() {
    return GHOST_PROPERTY;
  }
}
