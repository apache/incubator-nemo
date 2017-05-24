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
package edu.snu.vortex.runtime.common.plan;

import edu.snu.vortex.compiler.frontend.Coder;
import edu.snu.vortex.runtime.common.RuntimeAttributeMap;
import edu.snu.vortex.utils.dag.Edge;
import edu.snu.vortex.utils.dag.Vertex;

/**
 * Represents the edge between vertices in a logical/physical plan in runtime.
 * @param <V> the vertex type.
 */
public class RuntimeEdge<V extends Vertex> extends Edge<V> {
  private final RuntimeAttributeMap edgeAttributes;
  private final Coder coder;

  /**
   * Constructs the edge given the below parameters.
   * @param runtimeEdgeId the id of this edge.
   * @param edgeAttributes to control the data flow on this edge.
   * @param src the source vertex.
   * @param dst the destination vertex.
   * @param coder coder.
   */
  public RuntimeEdge(final String runtimeEdgeId,
                     final RuntimeAttributeMap edgeAttributes,
                     final V src,
                     final V dst,
                     final Coder coder) {
    super(runtimeEdgeId, src, dst);
    this.edgeAttributes = edgeAttributes;
    this.coder = coder;
  }

  public final RuntimeAttributeMap getEdgeAttributes() {
    return edgeAttributes;
  }

  public final Coder getCoder() {
    return coder;
  }

  /**
   * @return JSON representation of additional properties
   */
  @Override
  @SuppressWarnings("checkstyle:designforextension")
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"runtimeEdgeId\": \"").append(getId());
    sb.append("\", \"edgeAttributes\": ").append(edgeAttributes);
    sb.append(", \"coder\": \"").append(coder.toString());
    sb.append("\"}");
    return sb.toString();
  }
}
