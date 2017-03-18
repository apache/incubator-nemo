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
package edu.snu.vortex.runtime.common.execplan;


import edu.snu.vortex.runtime.common.*;
import edu.snu.vortex.runtime.common.RuntimeAttributes;

import java.util.Map;

/**
 * Runtime Edge.
 */
public final class RuntimeEdge {
  private final String runtimeEdgeId;
  private final Map<RuntimeAttributes.RuntimeEdgeAttribute, Object> edgeAttributes;
  private final String srcRuntimeVertexId;
  private final String dstRuntimeVertexId;

  /**
   * Represents the edge between vertices in an execution plan.
   * @param irEdgeId id of this edge.
   * @param edgeAttributes to control the data flow on this edge.
   * @param srcRuntimeVertexId source vertex.
   * @param dstRuntimeVertexId destination vertex.
   */
  public RuntimeEdge(final String irEdgeId,
                     final Map<RuntimeAttributes.RuntimeEdgeAttribute, Object> edgeAttributes,
                     final String srcRuntimeVertexId,
                     final String dstRuntimeVertexId) {
    this.runtimeEdgeId = RuntimeIdGenerator.generateRuntimeEdgeId(irEdgeId);
    this.edgeAttributes = edgeAttributes;
    this.srcRuntimeVertexId = srcRuntimeVertexId;
    this.dstRuntimeVertexId = dstRuntimeVertexId;
  }

  public String getId() {
    return runtimeEdgeId;
  }

  public Map<RuntimeAttributes.RuntimeEdgeAttribute, Object> getEdgeAttributes() {
    return edgeAttributes;
  }

  public String getSrcRuntimeVertexId() {
    return srcRuntimeVertexId;
  }

  public String getDstRuntimeVertexId() {
    return dstRuntimeVertexId;
  }
}
