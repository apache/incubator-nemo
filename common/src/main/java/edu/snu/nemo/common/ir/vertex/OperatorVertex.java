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
package edu.snu.nemo.common.ir.vertex;

import edu.snu.nemo.common.ir.vertex.transform.Transform;

/**
 * IRVertex that transforms input data.
 * It is to be constructed in the compiler frontend with language-specific data transform logic.
 */
public final class OperatorVertex extends IRVertex {
  private final Transform transform;

  /**
   * Constructor of OperatorVertex.
   * @param t transform for the OperatorVertex.
   */
  public OperatorVertex(final Transform t) {
    super();
    this.transform = t;
  }

  @Override
  public OperatorVertex getClone() {
    final OperatorVertex that = new OperatorVertex(this.transform);
    this.copyExecutionPropertiesTo(that);
    return that;
  }

  /**
   * @return the transform in the OperatorVertex.
   */
  public Transform getTransform() {
    return transform;
  }

  @Override
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append(irVertexPropertiesToString());
    sb.append(", \"transform\": \"");
    sb.append(transform);
    sb.append("\"}");
    return sb.toString();
  }
}
