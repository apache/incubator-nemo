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
package edu.snu.vortex.runtime.common.plan.physical;


import edu.snu.vortex.runtime.common.RuntimeAttributeMap;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex;

import java.io.Serializable;

/**
 * Contains information stage boundary {@link edu.snu.vortex.runtime.common.plan.logical.StageEdge}.
 */
public final class PhysicalStageEdge extends RuntimeEdge<PhysicalStage> implements Serializable {
  /**
   * The source {@link edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex}.
   * This could either belong to the external stage or this stage.
   */
  private final RuntimeVertex srcVertex;

  /**
   * The destination {@link edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex}.
   * This could either belong to the external stage or this stage.
   */
  private final RuntimeVertex dstVertex;

  /**
   * IRVertex attributes of the endpoint vertex.
   */
  private final RuntimeAttributeMap externalVertexAttr;

  public PhysicalStageEdge(final String runtimeEdgeId,
                           final RuntimeAttributeMap edgeAttributes,
                           final RuntimeVertex srcVertex,
                           final RuntimeVertex dstVertex,
                           final RuntimeAttributeMap externalVertexAttr,
                           final PhysicalStage srcStage,
                           final PhysicalStage dstStage) {
    super(runtimeEdgeId, edgeAttributes, srcStage, dstStage);
    this.srcVertex = srcVertex;
    this.dstVertex = dstVertex;
    this.externalVertexAttr = externalVertexAttr;
  }

  public RuntimeVertex getSrcVertex() {
    return srcVertex;
  }

  public RuntimeVertex getDstVertex() {
    return dstVertex;
  }

  public RuntimeAttributeMap getExternalVertexAttr() {
    return externalVertexAttr;
  }

  @Override
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"runtimeEdgeId\": \"").append(getId());
    sb.append("\", \"edgeAttributes\": ").append(getEdgeAttributes());
    sb.append(", \"externalSrcVertexId\": \"").append(srcVertex.getId());
    sb.append("\", \"externalDstVertexId\": \"").append(dstVertex.getId());
    sb.append("\", \"externalVertexAttr\": ").append(externalVertexAttr);
    sb.append("}");
    return sb.toString();
  }
}
