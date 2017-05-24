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
package edu.snu.vortex.runtime.common.plan.logical;


import edu.snu.vortex.compiler.frontend.Coder;
import edu.snu.vortex.runtime.common.RuntimeAttributeMap;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;

/**
 * Stage Edge.
 */
public final class StageEdge extends RuntimeEdge<Stage> {
  private final RuntimeVertex srcRuntimeVertex;
  private final RuntimeVertex dstRuntimeVertex;

  /**
   * Represents the edge between vertices in a logical plan.
   * @param irEdgeId id of this edge.
   * @param edgeAttributes to control the data flow on this edge.
   * @param srcStage source runtime stage.
   * @param dstStage destination runtime stage.
   * @param coder coder.
   * @param srcRuntimeVertex source vertex (in srcStage).
   * @param dstRuntimeVertex destination vertex (in dstStage).
   */
  public StageEdge(final String irEdgeId,
                   final RuntimeAttributeMap edgeAttributes,
                   final Stage srcStage,
                   final Stage dstStage,
                   final Coder coder,
                   final RuntimeVertex srcRuntimeVertex,
                   final RuntimeVertex dstRuntimeVertex) {
    super(RuntimeIdGenerator.generateStageEdgeId(irEdgeId), edgeAttributes, srcStage, dstStage, coder);
    this.srcRuntimeVertex = srcRuntimeVertex;
    this.dstRuntimeVertex = dstRuntimeVertex;
  }

  public RuntimeVertex getSrcRuntimeVertex() {
    return srcRuntimeVertex;
  }

  public RuntimeVertex getDstRuntimeVertex() {
    return dstRuntimeVertex;
  }

  @Override
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"runtimeEdgeId\": \"").append(getId());
    sb.append("\", \"edgeAttributes\": ").append(getEdgeAttributes());
    sb.append(", \"srcRuntimeVertex\": \"").append(srcRuntimeVertex.getId());
    sb.append("\", \"dstRuntimeVertex\": \"").append(dstRuntimeVertex.getId());
    sb.append("\", \"coder\": \"").append(getCoder().toString());
    sb.append("\"}");
    return sb.toString();
  }
}
