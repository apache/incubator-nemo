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
package org.apache.nemo.common.ir.vertex.executionproperty;

import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;

/**
 * A property represents that a vertex annotated with this property doesn't process any data and
 * should be regarded as a kind of "marker" to construct a temporary edge that contains some data that
 * have to be written before it's usage is not determined (e.g., for caching).
 * The written data in the edge toward the vertex annotated with this property will be used when
 * the usage is determined by using {@link org.apache.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupProperty}.
 * In that case, the edge will be regarded as a representative edge.
 * Attaching this property makes runtime to not schedule this vertex.
 */
public final class IgnoreSchedulingTempDataReceiverProperty extends VertexExecutionProperty<Boolean> {

  private static final IgnoreSchedulingTempDataReceiverProperty IGNORE_SCHEDULING_TEMP_DATA_RECEIVER_PROPERTY =
    new IgnoreSchedulingTempDataReceiverProperty(true);

  /**
   * Constructor.
   */
  private IgnoreSchedulingTempDataReceiverProperty(final Boolean value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @return the execution property.
   */
  public static IgnoreSchedulingTempDataReceiverProperty of() {
    return IGNORE_SCHEDULING_TEMP_DATA_RECEIVER_PROPERTY;
  }

  /**
   * Static method exposing the constructor.
   * This is used by reflection by the MetricUtils class.
   *
   * @param value the boolean value. This is always true by default for this property.
   * @return the new execution property.
   */
  public static IgnoreSchedulingTempDataReceiverProperty of(final Boolean value) {
    return new IgnoreSchedulingTempDataReceiverProperty(value);
  }
}
