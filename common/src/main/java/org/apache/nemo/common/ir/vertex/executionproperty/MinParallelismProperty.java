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
 * This property decides the minimum number of parallel tasks to use for executing the corresponding IRVertex.
 *
 * The actual number of tasks used in the execution can be greater due the following reasons.
 * - A larger number of parallelism of a parent IRVertex connected with an one-to-one IREdge.
 * - A larger number of source (e.g., HDFS) input data partitions. (SourceVertex)
 * - A larger size of the PartitionSet property of the input edge.
 */
public final class MinParallelismProperty extends VertexExecutionProperty<Integer> {
  /**
   * Constructor.
   *
   * @param value value of the execution property.
   */
  private MinParallelismProperty(final Integer value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static MinParallelismProperty of(final Integer value) {
    return new MinParallelismProperty(value);
  }
}
