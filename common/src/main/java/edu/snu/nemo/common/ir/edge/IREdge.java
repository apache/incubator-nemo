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
package edu.snu.nemo.common.ir.edge;

import edu.snu.nemo.common.dag.Edge;
import edu.snu.nemo.common.ir.IdManager;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.executionproperty.EdgeExecutionProperty;
import edu.snu.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import edu.snu.nemo.common.ir.vertex.IRVertex;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Physical execution plan of intermediate data movement.
 */
public final class IREdge extends Edge<IRVertex> {
  private final ExecutionPropertyMap<EdgeExecutionProperty> executionProperties;
  private final Boolean isSideInput;

  /**
   * Constructor of IREdge.
   * This constructor assumes that this edge is not for a side input.
   *
   * @param commPattern data communication pattern type of the edge.
   * @param src         source vertex.
   * @param dst         destination vertex.
   */
  public IREdge(final DataCommunicationPatternProperty.Value commPattern,
                final IRVertex src,
                final IRVertex dst) {
    this(commPattern, src, dst, false);
  }

  /**
   * Constructor of IREdge.
   *
   * @param commPattern data communication pattern type of the edge.
   * @param src         source vertex.
   * @param dst         destination vertex.
   * @param isSideInput flag for whether or not the edge is a sideInput.
   */
  public IREdge(final DataCommunicationPatternProperty.Value commPattern,
                final IRVertex src,
                final IRVertex dst,
                final Boolean isSideInput) {
    super(IdManager.newEdgeId(), src, dst);
    this.isSideInput = isSideInput;
    this.executionProperties = ExecutionPropertyMap.of(this, commPattern);
  }

  /**
   * Set an executionProperty of the IREdge.
   *
   * @param executionProperty the execution property.
   * @return the IREdge with the execution property set.
   */
  public IREdge setProperty(final EdgeExecutionProperty<?> executionProperty) {
    executionProperties.put(executionProperty);
    return this;
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
  public ExecutionPropertyMap getExecutionProperties() {
    return executionProperties;
  }

  /**
   * @return whether or not the edge is a side input edge.
   */
  public Boolean isSideInput() {
    return isSideInput;
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
    this.getExecutionProperties().forEachProperties((Consumer<EdgeExecutionProperty>) thatEdge::setProperty);
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

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(getSrc().hashCode())
        .append(getDst().hashCode())
        .append(executionProperties)
        .toHashCode();
  }

  @Override
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"id\": \"").append(getId());
    sb.append("\", \"executionProperties\": ").append(executionProperties);
    sb.append("}");
    return sb.toString();
  }
}
