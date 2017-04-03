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
import java.io.Serializable;

/**
 * Contains information stage boundary {@link edu.snu.vortex.runtime.common.plan.logical.RuntimeEdge}.
 */
public final class StageBoundaryEdgeInfo implements Serializable {
  private final String stageBoundaryEdgeInfoId;
  private final RuntimeAttributeMap edgeAttributes;

  /**
   * The ID of the endpoint {@link edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex} in the stage.
   * The vertex is connected to a vertex of another stage connected by the edge this class represents.
   */
  private final String externalEndpointVertexId;

  /**
   * Vertex attributes of the endpoint vertex.
   */
  private final RuntimeAttributeMap externalEndpointVertexAttr;

  public StageBoundaryEdgeInfo(final String runtimeEdgeId,
                         final RuntimeAttributeMap edgeAttributes,
                         final String externalEndpointVertexId,
                         final RuntimeAttributeMap externalEndpointVertexAttr) {
    this.stageBoundaryEdgeInfoId = runtimeEdgeId;
    this.edgeAttributes = edgeAttributes;
    this.externalEndpointVertexId = externalEndpointVertexId;
    this.externalEndpointVertexAttr = externalEndpointVertexAttr;
  }

  public String getStageBoundaryEdgeInfoId() {
    return stageBoundaryEdgeInfoId;
  }

  public RuntimeAttributeMap getEdgeAttributes() {
    return edgeAttributes;
  }

  public String getExternalEndpointVertexId() {
    return externalEndpointVertexId;
  }

  public RuntimeAttributeMap getExternalEndpointVertexAttr() {
    return externalEndpointVertexAttr;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("StageBoundaryEdgeInfo{");
    sb.append("stageBoundaryEdgeInfoId='").append(stageBoundaryEdgeInfoId).append('\'');
    sb.append(", edgeAttributes=").append(edgeAttributes);
    sb.append(", externalEndpointVertexId='").append(externalEndpointVertexId).append('\'');
    sb.append(", externalEndpointVertexAttr=").append(externalEndpointVertexAttr);
    sb.append('}');
    return sb.toString();
  }
}
