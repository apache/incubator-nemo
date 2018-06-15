/*
 * Copyright (C) 2018 Seoul National University
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

import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.common.dag.DAGBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Stage Builder.
 */
final class StageBuilder {
  private final DAGBuilder<IRVertex, RuntimeEdge<IRVertex>> stageInternalDAGBuilder;
  private final Integer stageId;
  private final int parallelism;
  private final int scheduleGroupIndex;
  private final String containerType;
  private List<Map<String, Readable>> vertexIdToReadables;

  /**
   * Builds a {@link Stage}.
   * @param stageId stage ID in numeric form.
   * @param scheduleGroupIndex indicating its scheduling order.
   */
  StageBuilder(final Integer stageId,
               final int parallelism,
               final int scheduleGroupIndex,
               final String containerType) {
    this.stageId = stageId;
    this.parallelism = parallelism;
    this.scheduleGroupIndex = scheduleGroupIndex;
    this.containerType = containerType;
    this.stageInternalDAGBuilder = new DAGBuilder<>();
    this.vertexIdToReadables = new ArrayList<>(1);
  }

  /**
   * Adds a {@link IRVertex} to this stage.
   * @param vertex to add.
   * @return the stageBuilder.
   */
  StageBuilder addVertex(final IRVertex vertex) {
    stageInternalDAGBuilder.addVertex(vertex);
    return this; }

  /**
   * Connects two {@link IRVertex} in this stage.
   * @param edge the IREdge that connects vertices.
   * @return the stageBuilder.
   */
  StageBuilder connectInternalVertices(final RuntimeEdge<IRVertex> edge) {
    stageInternalDAGBuilder.connectVertices(edge);
    return this;
  }

  StageBuilder addReadables(final List<Map<String, Readable>> vertexIdToReadable) {
    this.vertexIdToReadables = vertexIdToReadable;
    return this;
  }

  /**
   * @return true if this builder doesn't contain any valid {@link IRVertex}.
   */
  boolean isEmpty() {
    return stageInternalDAGBuilder.isEmpty();
  }

  /**
   * Builds and returns the {@link Stage}.
   * @return the runtime stage.
   */
  Stage build() {
    final Stage stage = new Stage(
        RuntimeIdGenerator.generateStageId(stageId),
        stageInternalDAGBuilder.buildWithoutSourceSinkCheck(),
        parallelism,
        scheduleGroupIndex,
        containerType,
        vertexIdToReadables);
    return stage;
  }
}
