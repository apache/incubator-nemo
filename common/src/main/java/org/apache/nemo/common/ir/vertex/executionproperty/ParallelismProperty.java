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
 * This property decides the number of parallel tasks to use for executing the corresponding IRVertex.
 * <p>
 * Changing the parallelism requires also changing other execution properties that refer to task offsets.
 * Such execution properties include:
 * {@link ResourceSiteProperty}
 * {@link ResourceAntiAffinityProperty}
 * {@link org.apache.nemo.common.ir.edge.executionproperty.PartitionerProperty}
 * {@link org.apache.nemo.common.ir.edge.executionproperty.PartitionSetProperty}
 * <p>
 * Moreover, vertices with one-to-one relationships must have the same parallelism.
 * {@link org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty}
 * <p>
 * Finally, the parallelism cannot be larger than the number of source (e.g., HDFS) input data partitions.
 * {@link org.apache.nemo.common.ir.vertex.SourceVertex}
 * <p>
 * A violation of any of the above criteria will be caught by Nemo, to ensure correct application semantics.
 */
public final class ParallelismProperty extends VertexExecutionProperty<Integer> {
  /**
   * Constructor.
   *
   * @param value value of the execution property.
   */
  private ParallelismProperty(final Integer value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static ParallelismProperty of(final Integer value) {
    return new ParallelismProperty(value);
  }
}
