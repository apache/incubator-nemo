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
package edu.snu.onyx.runtime.common.plan;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.common.dag.Edge;
import edu.snu.onyx.common.dag.Vertex;
import edu.snu.onyx.common.ir.executionproperty.ExecutionPropertyMap;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;

/**
 * Represents the edge between vertices in a logical/physical plan in runtime.
 * @param <V> the vertex type.
 */
public class RuntimeEdge<V extends Vertex> extends Edge<V> {
  private final ExecutionPropertyMap edgeProperties;
  private final Coder coder;
  private final Boolean isSideInput;

  /**
   * Constructs the edge given the below parameters.
   * @param runtimeEdgeId the id of this edge.
   * @param edgeProperties to control the data flow on this edge.
   * @param src the source vertex.
   * @param dst the destination vertex.
   * @param coder coder.
   */
  public RuntimeEdge(final String runtimeEdgeId,
                     final ExecutionPropertyMap edgeProperties,
                     final V src,
                     final V dst,
                     final Coder coder) {
    this(runtimeEdgeId, edgeProperties, src, dst, coder, false);
  }

  /**
   * Constructs the edge given the below parameters.
   * @param runtimeEdgeId the id of this edge.
   * @param edgeProperties to control the data flow on this edge.
   * @param src the source vertex.
   * @param dst the destination vertex.
   * @param coder coder.
   * @param isSideInput Whether or not the RuntimeEdge is a side input edge.
   */
  public RuntimeEdge(final String runtimeEdgeId,
                     final ExecutionPropertyMap edgeProperties,
                     final V src,
                     final V dst,
                     final Coder coder,
                     final Boolean isSideInput) {
    super(runtimeEdgeId, src, dst);
    this.edgeProperties = edgeProperties;
    this.coder = coder;
    this.isSideInput = isSideInput;
  }

  /**
   * Get the execution property of the Runtime Edge.
   * @param <T> Type of the return value.
   * @param executionPropertyKey key of the execution property.
   * @return the execution property.
   */
  public final <T> T getProperty(final ExecutionProperty.Key executionPropertyKey) {
    return edgeProperties.get(executionPropertyKey);
  }

  /**
   * @return the ExecutionPropertyMap of the Runtime Edge.
   */
  public final ExecutionPropertyMap getExecutionProperties() {
    return edgeProperties;
  }

  /**
   * @return the coder for encoding and decoding.
   */
  public final Coder getCoder() {
    return coder;
  }

  /**
   * @return whether or not the RuntimeEdge is a side input edge.
   */
  public final Boolean isSideInput() {
    return isSideInput;
  }

  /**
   * @return JSON representation of additional properties
   */
  @Override
  @SuppressWarnings("checkstyle:designforextension")
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"runtimeEdgeId\": \"").append(getId());
    sb.append("\", \"edgeProperties\": ").append(edgeProperties);
    sb.append(", \"coder\": \"").append(coder.toString());
    sb.append("\"}");
    return sb.toString();
  }
}
