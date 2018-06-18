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
package edu.snu.nemo.runtime.common.plan;

import edu.snu.nemo.common.dag.Edge;
import edu.snu.nemo.common.dag.Vertex;
import edu.snu.nemo.common.ir.executionproperty.EdgeExecutionProperty;
import edu.snu.nemo.common.ir.executionproperty.ExecutionPropertyMap;

import java.io.Serializable;
import java.util.Optional;

/**
 * Represents the edge between vertices in a logical/physical plan in runtime.
 * @param <V> the vertex type.
 */
public class RuntimeEdge<V extends Vertex> extends Edge<V> {
  private final ExecutionPropertyMap executionProperties;
  private final Boolean isSideInput;

  /**
   * Constructs the edge given the below parameters.
   *
   * @param runtimeEdgeId  the id of this edge.
   * @param executionProperties to control the data flow on this edge.
   * @param src            the source vertex.
   * @param dst            the destination vertex.
   * @param isSideInput    Whether or not the RuntimeEdge is a side input edge.
   */
  public RuntimeEdge(final String runtimeEdgeId,
                     final ExecutionPropertyMap executionProperties,
                     final V src,
                     final V dst,
                     final Boolean isSideInput) {
    super(runtimeEdgeId, src, dst);
    this.executionProperties = executionProperties;
    this.isSideInput = isSideInput;
  }

  /**
   * Get the execution property of the Runtime Edge.
   *
   * @param <T>                  Type of the return value.
   * @param executionPropertyKey key of the execution property.
   * @return the execution property.
   */
  public final <T extends Serializable> Optional<T> getPropertyValue(
      final Class<? extends EdgeExecutionProperty<T>> executionPropertyKey) {
    return executionProperties.get(executionPropertyKey);
  }

  /**
   * @return the ExecutionPropertyMap of the Runtime Edge.
   */
  public final ExecutionPropertyMap getExecutionProperties() {
    return executionProperties;
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
    sb.append("\", \"executionProperties\": ").append(executionProperties);
    sb.append("}");
    return sb.toString();
  }
}
