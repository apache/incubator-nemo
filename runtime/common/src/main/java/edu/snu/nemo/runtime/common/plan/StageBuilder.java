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
package edu.snu.nemo.runtime.common.plan;

import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.common.dag.DAGBuilder;

/**
 * Stage Builder.
 */
public final class StageBuilder {
  private final DAGBuilder<IRVertex, IREdge> stageInternalDAGBuilder;
  private final Integer stageId;
  private final int scheduleGroupIndex;

  /**
   * Builds a {@link Stage}.
   * @param stageId stage ID in numeric form.
   * @param scheduleGroupIndex indicating its scheduling order.
   */
  public StageBuilder(final Integer stageId,
                      final int scheduleGroupIndex) {
    this.stageId = stageId;
    this.scheduleGroupIndex = scheduleGroupIndex;
    this.stageInternalDAGBuilder = new DAGBuilder<>();
  }

  /**
   */
  /**
   * Adds a {@link IRVertex} to this stage.
   * @param vertex to add.
   * @return the stageBuilder.
   */
  public StageBuilder addVertex(final IRVertex vertex) {
    stageInternalDAGBuilder.addVertex(vertex);
    return this;
  }

  /**
   * Connects two {@link IRVertex} in this stage.
   * @param edge the IREdge that connects vertices.
   * @return the stageBuilder.
   */
  public StageBuilder connectInternalVertices(final IREdge edge) {
    stageInternalDAGBuilder.connectVertices(edge);
    return this;
  }

  /**
   * @return true if this builder doesn't contain any valid {@link IRVertex}.
   */
  public boolean isEmpty() {
    return stageInternalDAGBuilder.isEmpty();
  }

  /**
   * Builds and returns the {@link Stage}.
   * @return the runtime stage.
   */
  public Stage build() {
    final Stage stage = new Stage(
        RuntimeIdGenerator.generateStageId(stageId),
        stageInternalDAGBuilder.buildWithoutSourceSinkCheck(),
        scheduleGroupIndex);
    return stage;
  }
}