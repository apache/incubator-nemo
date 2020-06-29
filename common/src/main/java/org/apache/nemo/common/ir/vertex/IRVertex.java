/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.common.ir.vertex;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nemo.common.Cloneable;
import org.apache.nemo.common.dag.Vertex;
import org.apache.nemo.common.ir.IdManager;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;

import java.io.Serializable;
import java.util.Optional;

/**
 * The basic unit of operation in a dataflow program, as well as the most important data structure in Nemo.
 * An IRVertex is created and modified in the compiler, and executed in the runtime.
 */
public abstract class IRVertex extends Vertex implements Cloneable<IRVertex> {
  private final ExecutionPropertyMap<VertexExecutionProperty> executionProperties;

  /**
   * Constructor of IRVertex.
   */
  public IRVertex() {
    super(IdManager.newVertexId());
    this.executionProperties = ExecutionPropertyMap.of(this);
  }

  /**
   * Copy Constructor for IRVertex.
   *
   * @param that the source object for copying
   */
  public IRVertex(final IRVertex that) {
    super(IdManager.getVertexId(that));
    this.executionProperties = ExecutionPropertyMap.of(this);
    that.copyExecutionPropertiesTo(this);
  }

  /**
   * Static function to copy executionProperties from a vertex to the other.
   *
   * @param thatVertex the edge to copy executionProperties to.
   */
  public final void copyExecutionPropertiesTo(final IRVertex thatVertex) {
    this.getExecutionProperties().forEachProperties(thatVertex::setProperty);
  }

  /**
   * Set an executionProperty of the IRVertex.
   *
   * @param executionProperty new execution property.
   * @return the IRVertex with the execution property set.
   */
  public final IRVertex setProperty(final VertexExecutionProperty<?> executionProperty) {
    executionProperties.put(executionProperty, false);
    return this;
  }

  /**
   * Set an executionProperty of the IRVertex, permanently.
   *
   * @param executionProperty new execution property.
   * @return the IRVertex with the execution property set.
   */
  public final IRVertex setPropertyPermanently(final VertexExecutionProperty<?> executionProperty) {
    executionProperties.put(executionProperty, true);
    return this;
  }

  public final Boolean isUtilityVertex() {
    return this.getClass().getPackage().getName().startsWith("org.apache.nemo.common.ir.vertex.utility.");
  }

  /**
   * Get the executionProperty of the IRVertex.
   *
   * @param <T>                  Type of the return value.
   * @param executionPropertyKey key of the execution property.
   * @return the execution property.
   */
  public final <T extends Serializable> Optional<T> getPropertyValue(
    final Class<? extends VertexExecutionProperty<T>> executionPropertyKey) {
    return executionProperties.get(executionPropertyKey);
  }

  /**
   * @return the ExecutionPropertyMap of the IRVertex.
   */
  public final ExecutionPropertyMap<VertexExecutionProperty> getExecutionProperties() {
    return executionProperties;
  }

  /**
   * @return IRVertex properties as JSON node.
   */
  protected final ObjectNode getIRVertexPropertiesAsJsonNode() {
    final ObjectMapper mapper = new ObjectMapper();
    final ObjectNode node = mapper.createObjectNode();
    node.put("class", getClass().getSimpleName());
    node.set("executionProperties", executionProperties.asJsonNode());
    return node;
  }
}
