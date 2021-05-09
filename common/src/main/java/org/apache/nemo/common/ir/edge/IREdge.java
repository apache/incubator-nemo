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
package org.apache.nemo.common.ir.edge;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.IdManager;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.vertex.IRVertex;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.*;

/**
 * Physical execution plan of intermediate data movement.
 */
public final class IREdge extends Edge<IRVertex> {
  private final ExecutionPropertyMap<EdgeExecutionProperty> executionProperties;

  /**
   * Constructor of IREdge.
   *
   * @param commPattern data communication pattern type of the edge.
   * @param src         source vertex.
   * @param dst         destination vertex.
   */
  public IREdge(final CommunicationPatternProperty.Value commPattern,
                final IRVertex src,
                final IRVertex dst) {
    super(IdManager.newEdgeId(), src, dst);
    this.executionProperties = ExecutionPropertyMap.of(this, commPattern);
  }

  /**
   * Set an executionProperty of the IREdge.
   * @param executionProperty the execution property.
   * @return the IREdge with the execution property set.
   */
  public IREdge setProperty(final EdgeExecutionProperty<?> executionProperty) {
    executionProperties.put(executionProperty, false);
    return this;
  }

  /**
   * Set an executionProperty of the IREdge, permanently.
   * @param executionProperty the execution property.
   * @return the IREdge with the execution property set.
   */
  public IREdge setPropertyPermanently(final EdgeExecutionProperty<?> executionProperty) {
    executionProperties.put(executionProperty, true);
    return this;
  }

  public boolean isTransientEdge() {
    return getPropertyValue(CommunicationPatternProperty.class)
      .get()
      .equals(CommunicationPatternProperty.Value.TransientShuffle) ||
      getPropertyValue(CommunicationPatternProperty.class)
        .get()
        .equals(CommunicationPatternProperty.Value.TransientOneToOne) ||
       getPropertyValue(CommunicationPatternProperty.class)
        .get()
        .equals(CommunicationPatternProperty.Value.TransientBroadcast) ||
      getPropertyValue(CommunicationPatternProperty.class)
        .get()
        .equals(CommunicationPatternProperty.Value.TransientRR);
  }


  /**
   * Get the executionProperty of the IREdge.
   *
   * @param <T>                  Type of the return value.
   * @param executionPropertyKey key of the execution property.
   * @return the execution property.
   */
  public <T extends Serializable> Optional<T> getPropertyValue(
      final Class<? extends EdgeExecutionProperty<T>> executionPropertyKey) {
    return executionProperties.get(executionPropertyKey);
  }

  /**
   * @return the ExecutionPropertyMap of the IREdge.
   */
  public ExecutionPropertyMap<EdgeExecutionProperty> getExecutionProperties() {
    return executionProperties;
  }

  /**
   * @param edge edge to compare.
   * @return whether or not the edge has the same itinerary
   */
  public Boolean hasSameItineraryAs(final IREdge edge) {
    return getSrc().equals(edge.getSrc()) && getDst().equals(edge.getDst());
  }

  /**
   * Static function to copy executionProperties from an edge to the other.
   *
   * @param thatEdge the edge to copy executionProperties to.
   */
  public void copyExecutionPropertiesTo(final IREdge thatEdge) {
    this.getExecutionProperties().forEachProperties(thatEdge::setProperty);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final IREdge irEdge = (IREdge) o;

    return executionProperties.equals(irEdge.getExecutionProperties()) && hasSameItineraryAs(irEdge);
  }

}
