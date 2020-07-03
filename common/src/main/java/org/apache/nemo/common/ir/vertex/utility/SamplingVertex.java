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

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.utility.runtimepass.MessageGeneratorVertex;

/**
 * Executes the original IRVertex using a subset of input data partitions.
 */
public final class SamplingVertex extends IRVertex {
  private final IRVertex originalVertex;
  private final IRVertex cloneOfOriginalVertex;
  private final float desiredSampleRate;

  /**
   * @param originalVertex    to clone.
   * @param desiredSampleRate percentage of tasks to execute.
   *                          The actual sample rate may vary depending on neighboring sampling vertices.
   */
  public SamplingVertex(final IRVertex originalVertex, final float desiredSampleRate) {
    super();
    if (!(originalVertex instanceof MessageGeneratorVertex) && (Util.isUtilityVertex(originalVertex))) {
      throw new IllegalArgumentException(
        "Cannot sample non-Trigger utility vertices: " + originalVertex.toString());
    }
    if (desiredSampleRate > 1 || desiredSampleRate <= 0) {
      throw new IllegalArgumentException(String.valueOf(desiredSampleRate));
    }
    this.originalVertex = originalVertex;
    this.cloneOfOriginalVertex = originalVertex.getClone();
    this.desiredSampleRate = desiredSampleRate;

    // Copy execution properties.
    originalVertex.copyExecutionPropertiesTo(cloneOfOriginalVertex);
    originalVertex.copyExecutionPropertiesTo(this);
  }

  /**
   * @return the id of the original vertex for reference.
   */
  public String getOriginalVertexId() {
    return originalVertex.getId();
  }

  /**
   * @return the clone of the original vertex.
   * This clone is intended to be used during the actual execution, as the sampling vertex itself is not executable
   * and the original vertex should not be executed again.
   */
  public IRVertex getCloneOfOriginalVertex() {
    copyExecutionPropertiesTo(cloneOfOriginalVertex); // reflect the updated EPs
    return cloneOfOriginalVertex;
  }

  /**
   * @return the desired sample rate.
   */
  public float getDesiredSampleRate() {
    return desiredSampleRate;
  }

  /**
   * Obtains a clone of an original edge that is attached to this sampling vertex.
   * <p>
   * Original edge: src - to - dst
   * When src == originalVertex, return thisSamplingVertex - to - dst
   * When dst == originalVertex, return src - to - thisSamplingVertex
   *
   * @param originalEdge to clone.
   * @return a clone of the edge.
   */
  public IREdge getCloneOfOriginalEdge(final IREdge originalEdge) {
    if (originalEdge.getSrc().equals(originalVertex)) {
      return Util.cloneEdge(originalEdge, this, originalEdge.getDst());
    } else if (originalEdge.getDst().equals(originalVertex)) {
      return Util.cloneEdge(originalEdge, originalEdge.getSrc(), this);
    } else {
      throw new IllegalArgumentException(originalEdge.getId());
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("SamplingVertex ");
    sb.append(getId());
    sb.append("(desiredSampleRate:");
    sb.append(String.valueOf(desiredSampleRate));
    sb.append(", ");
    sb.append(getOriginalVertexId());
    sb.append(")");
    return sb.toString();
  }

  @Override
  public IRVertex getClone() {
    return new SamplingVertex(originalVertex, desiredSampleRate);
  }

  @Override
  public ObjectNode getPropertiesAsJsonNode() {
    final ObjectNode node = getIRVertexPropertiesAsJsonNode();
    node.put("transform", toString());
    return node;
  }
}
