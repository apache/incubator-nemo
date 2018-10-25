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
package org.apache.nemo.runtime.common.plan;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.dag.Vertex;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;

import java.io.Serializable;
import java.util.Optional;

/**
 * Represents the edge between vertices in a logical/physical plan in runtime.
 * @param <V> the vertex type.
 */
public class RuntimeEdge<V extends Vertex> extends Edge<V> {
  private final ExecutionPropertyMap<EdgeExecutionProperty> executionProperties;

  /**
   * Constructs the edge given the below parameters.
   *
   * @param runtimeEdgeId  the id of this edge.
   * @param executionProperties to control the data flow on this edge.
   * @param src            the source vertex.
   * @param dst            the destination vertex.
   */
  public RuntimeEdge(final String runtimeEdgeId,
                     final ExecutionPropertyMap<EdgeExecutionProperty> executionProperties,
                     final V src,
                     final V dst) {
    super(runtimeEdgeId, src, dst);
    this.executionProperties = executionProperties;
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
  public final ExecutionPropertyMap<EdgeExecutionProperty> getExecutionProperties() {
    return executionProperties;
  }

  /**
   * @return JSON representation of additional properties
   */
  @Override
  @SuppressWarnings("checkstyle:designforextension")
  public ObjectNode getPropertiesAsJsonNode() {
    final ObjectNode node = JsonNodeFactory.instance.objectNode();
    node.put("runtimeEdgeId", getId());
    node.set("executionProperties", executionProperties.asJsonNode());
    return node;
  }
}
