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

import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.runtime.common.RuntimeAttributeMap;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.common.dag.DAGBuilder;

/**
 * Stage Builder.
 */
public final class StageBuilder {
  private final DAGBuilder<RuntimeVertex, RuntimeEdge<RuntimeVertex>> stageInternalDAGBuilder;

  /**
   * Builds a {@link Stage}.
   */
  public StageBuilder() {
    this.stageInternalDAGBuilder = new DAGBuilder<>();
  }

  /**
   * Adds a {@link RuntimeVertex} to this stage.
   * @param vertex to add.
   */
  public void addRuntimeVertex(final RuntimeVertex vertex) {
    stageInternalDAGBuilder.addVertex(vertex);
  }

  /**
   * Connects two {@link RuntimeVertex} in this stage.
   * @param irEdgeId the IR edge ID to be used for this edge.
   * @param edgeAttributes edge attributes of the edge.
   * @param srcVertex source vertex.
   * @param dstVertex destination vertex.
   * @param coder coder.
   */
  public void connectInternalRuntimeVertices(final String irEdgeId,
                                             final RuntimeAttributeMap edgeAttributes,
                                             final RuntimeVertex srcVertex,
                                             final RuntimeVertex dstVertex,
                                             final Coder coder) {
    final RuntimeEdge<RuntimeVertex> edge = new RuntimeEdge<>(RuntimeIdGenerator.generateRuntimeEdgeId(irEdgeId),
        edgeAttributes, srcVertex, dstVertex, coder);
    stageInternalDAGBuilder.connectVertices(edge);
  }

  /**
   * @return true if this builder contains any valid {@link RuntimeVertex}.
   */
  public boolean isEmpty() {
    return stageInternalDAGBuilder.isEmpty();
  }

  /**
   * Builds and returns the {@link Stage}.
   * @return the runtime stage.
   */
  public Stage build() {
    return new Stage(RuntimeIdGenerator.generateStageId(), stageInternalDAGBuilder.build());
  }
}
