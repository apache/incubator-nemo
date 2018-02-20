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
package edu.snu.nemo.runtime.common.plan.stage;


import edu.snu.nemo.common.coder.Coder;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;

/**
 * Stage Edge.
 */
public final class StageEdge extends RuntimeEdge<Stage> {
  private final IRVertex srcVertex;
  private final IRVertex dstVertex;

  /**
   * Represents the edge between stages.
   * @param irEdgeId id of this edge.
   * @param edgeProperties to control the data flow on this edge.
   * @param srcStage source runtime stage.
   * @param dstStage destination runtime stage.
   * @param coder coder.
   * @param isSideInput flag for whether or not the edge is a sideInput.
   * @param srcVertex source vertex (in srcStage).
   * @param dstVertex destination vertex (in dstStage).
   */
  public StageEdge(final String irEdgeId,
                   final ExecutionPropertyMap edgeProperties,
                   final Stage srcStage,
                   final Stage dstStage,
                   final Coder coder,
                   final Boolean isSideInput,
                   final IRVertex srcVertex,
                   final IRVertex dstVertex) {
    super(RuntimeIdGenerator.generateStageEdgeId(irEdgeId), edgeProperties, srcStage, dstStage, coder, isSideInput);
    this.srcVertex = srcVertex;
    this.dstVertex = dstVertex;
  }

  /**
   * @return the source vertex.
   */
  public IRVertex getSrcVertex() {
    return srcVertex;
  }

  /**
   * @return the destination vertex.
   */
  public IRVertex getDstVertex() {
    return dstVertex;
  }

  @Override
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"runtimeEdgeId\": \"").append(getId());
    sb.append("\", \"edgeProperties\": ").append(getExecutionProperties());
    sb.append(", \"srcVertex\": \"").append(srcVertex.getId());
    sb.append("\", \"dstVertex\": \"").append(dstVertex.getId());
    sb.append("\", \"coder\": \"").append(getCoder().toString());
    sb.append("\"}");
    return sb.toString();
  }
}
