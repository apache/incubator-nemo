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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;

/**
 * Executes the original IRVertex using a subset of input data partitions.
 */
public final class SamplingVertex extends IRVertex {
  private final IRVertex originalVertex;
  private final IRVertex cloneOfOriginalVertex;
  private final float desiredSampleRate;

  /**
   * @param originalVertex to clone.
   * @param desiredSampleRate percentage of tasks to execute.
   *                          The actual sample rate may vary depending on neighboring sampling vertices.
   */
  public SamplingVertex(final IRVertex originalVertex, final float desiredSampleRate) {
    super();
    if (originalVertex instanceof SamplingVertex) {
      throw new IllegalArgumentException("Cannot sample again: " + originalVertex.toString());
    }
    if (desiredSampleRate > 1 || desiredSampleRate <= 0) {
      throw new IllegalArgumentException(String.valueOf(desiredSampleRate));
    }
    this.originalVertex = originalVertex;
    this.cloneOfOriginalVertex = originalVertex.getClone();
    this.desiredSampleRate = desiredSampleRate;
    originalVertex.copyExecutionPropertiesTo(this);
  }

  public IRVertex getOriginalVertex() {
    return originalVertex;
  }

  public IRVertex getCloneOfOriginalVertex() {
    this.copyExecutionPropertiesTo(cloneOfOriginalVertex);
    return cloneOfOriginalVertex;
  }

  public float getDesiredSampleRate() {
    return desiredSampleRate;
  }

  /**
   * Obtains a clone of an original edge that is attached to this sampling vertex.
   *
   * Original edge: src - to - dst
   * When src == originalVertex, return thisSamplingVertex - to - dst
   * When dst == originalVertex, return src - to - thisSamplingVertex
   *
   * @param originalEdge to clone.
   * @return a clone of the edge.
   */
  public IREdge getCloneOfOriginalEdge(final IREdge originalEdge) {
    if (originalEdge.getSrc().equals(originalVertex)) {
      return new IREdge(
        originalEdge.getPropertyValue(CommunicationPatternProperty.class).get(), this, originalEdge.getDst());
    } else if (originalEdge.getDst().equals(originalVertex)) {
      return new IREdge(
        originalEdge.getPropertyValue(CommunicationPatternProperty.class).get(), originalEdge.getSrc(), this);
    } else {
      throw new IllegalArgumentException(originalEdge.getId());
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("SamplingVertex(desiredSampleRate:");
    sb.append(String.valueOf(desiredSampleRate));
    sb.append(")[");
    sb.append(originalVertex.toString());
    sb.append("]");
    return sb.toString();
  }

  @Override
  public IRVertex getClone() {
    return new SamplingVertex(originalVertex, desiredSampleRate);
  }

  @Override
  public final JsonNode getPropertiesAsJsonNode() {
    return originalVertex.getPropertiesAsJsonNode();
  }
}
