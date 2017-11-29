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
package edu.snu.onyx.common.ir.vertex;

import edu.snu.onyx.common.ir.IdManager;
import edu.snu.onyx.common.ir.executionproperty.ExecutionPropertyMap;
import edu.snu.onyx.common.dag.Vertex;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;

/**
 * The top-most wrapper for a user operation in the IR.
 */
public abstract class IRVertex extends Vertex {
  private final ExecutionPropertyMap executionProperties;

  /**
   * Constructor of IRVertex.
   */
  public IRVertex() {
    super(IdManager.newVertexId());
    this.executionProperties = ExecutionPropertyMap.of(this);
  }

  /**
   * @return a clone elemnt of the IRVertex.
   */
  public abstract IRVertex getClone();

  /**
   * Static function to copy executionProperties from a vertex to the other.
   * @param thatVertex the edge to copy executionProperties to.
   */
  public final void copyExecutionPropertiesTo(final IRVertex thatVertex) {
    this.getExecutionProperties().forEachProperties(thatVertex::setProperty);
  }

  /**
   * Set an executionProperty of the IRVertex.
   * @param executionProperty new execution property.
   * @return the IRVertex with the execution property set.
   */
  public final IRVertex setProperty(final ExecutionProperty<?> executionProperty) {
    executionProperties.put(executionProperty);
    return this;
  }

  /**
   * Get the executionProperty of the IRVertex.
   * @param <T> Type of the return value.
   * @param executionPropertyKey key of the execution property.
   * @return the execution property.
   */
  public final <T> T getProperty(final ExecutionProperty.Key executionPropertyKey) {
    return executionProperties.get(executionPropertyKey);
  }

  /**
   * @return the ExecutionPropertyMap of the IRVertex.
   */
  public final ExecutionPropertyMap getExecutionProperties() {
    return executionProperties;
  }

  /**
   * @return IRVertex properties in String form.
   */
  protected final String irVertexPropertiesToString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("\"class\": \"").append(this.getClass().getSimpleName());
    sb.append("\", \"executionProperties\": ").append(executionProperties);
    return sb.toString();
  }
}
