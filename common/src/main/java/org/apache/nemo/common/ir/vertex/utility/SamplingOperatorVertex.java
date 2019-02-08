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
package org.apache.nemo.common.ir.vertex.utility;

import org.apache.nemo.common.ir.vertex.OperatorVertex;

/**
 * Executes the original IRVertex using a randomly sampled subset of input data partitions.
 */
public final class SamplingOperatorVertex extends OperatorVertex {
  private OperatorVertex originalVertex;
  private float desiredSampleRate;

  /**
   * Constructor.
   */
  public SamplingOperatorVertex(final OperatorVertex originalVertex,
                                final float desiredSampleRate) {
    super(originalVertex.getTransform()); // TODO: indicate number of actual tasks to use --> to be used in "Stage"
    this.originalVertex = originalVertex;
    this.desiredSampleRate = desiredSampleRate;
  }

  public OperatorVertex getOriginalVertex() {
    return originalVertex;
  }
}
