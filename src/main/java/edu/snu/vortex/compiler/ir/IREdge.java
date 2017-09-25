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
package edu.snu.vortex.compiler.ir;

import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionPropertyMap;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.ir.executionproperty.edge.DataCommunicationPatternProperty;
import edu.snu.vortex.runtime.exception.UnsupportedExecutionPropertyException;
import edu.snu.vortex.common.dag.Edge;
import edu.snu.vortex.runtime.executor.datatransfer.data_communication_pattern.Broadcast;
import edu.snu.vortex.runtime.executor.datatransfer.data_communication_pattern.OneToOne;
import edu.snu.vortex.runtime.executor.datatransfer.data_communication_pattern.ScatterGather;

/**
 * Physical execution plan of intermediate data movement.
 */
public final class IREdge extends Edge<IRVertex> {
  /**
   * Type of edges.
   */
  public enum Type {
    OneToOne,
    Broadcast,
    ScatterGather,
  }

  private final ExecutionPropertyMap executionProperties;
  private final Type type;
  private final Coder coder;

  /**
   * Constructor of IREdge.
   * @param type type of the edge.
   * @param src source vertex.
   * @param dst destination vertex.
   * @param coder coder.
   */
  public IREdge(final Type type,
                final IRVertex src,
                final IRVertex dst,
                final Coder coder) {
    super(IdManager.newEdgeId(), src, dst);
    this.type = type;
    this.coder = coder;
    this.executionProperties = ExecutionPropertyMap.of(this);
    switch (this.getType()) {
      case OneToOne:
        setProperty(DataCommunicationPatternProperty.of(OneToOne.class));
        break;
      case Broadcast:
        setProperty(DataCommunicationPatternProperty.of(Broadcast.class));
        break;
      case ScatterGather:
        setProperty(DataCommunicationPatternProperty.of(ScatterGather.class));
        break;
      default:
        throw new UnsupportedExecutionPropertyException("There is no such edge type as: " + this.getType());
    }
  }

  /**
   * Set an executionProperty of the IREdge.
   * @param executionProperty the execution property.
   * @return the IREdge with the execution property set.
   */
  public IREdge setProperty(final ExecutionProperty<?> executionProperty) {
    executionProperties.put(executionProperty);
    return this;
  }

  /**
   * Get the executionProperty of the IREdge.
   * @param <T> Type of the return value.
   * @param executionPropertyKey key of the execution property.
   * @return the execution property.
   */
  public <T> T get(final ExecutionProperty.Key executionPropertyKey) {
    return executionProperties.get(executionPropertyKey);
  }

  /**
   * @return the ExecutionPropertyMap of the IREdge.
   */
  public ExecutionPropertyMap getExecutionProperties() {
    return executionProperties;
  }

  /**
   * @return type of the edge.
   */
  public Type getType() {
    return type;
  }

  /**
   * @return coder for the edge.
   */
  public Coder getCoder() {
    return coder;
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

    IREdge irEdge = (IREdge) o;

    return type.equals(irEdge.getType()) && hasSameItineraryAs(irEdge);
  }

  @Override
  public int hashCode() {
    int result = type.hashCode();
    result = 31 * result + getSrc().hashCode();
    result = 31 * result + getDst().hashCode();
    return result;
  }

  @Override
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"id\": \"").append(getId());
    sb.append("\", \"executionProperties\": ").append(executionProperties);
    sb.append(", \"type\": \"").append(type);
    sb.append("\", \"coder\": \"").append(coder.toString());
    sb.append("\"}");
    return sb.toString();
  }
}
