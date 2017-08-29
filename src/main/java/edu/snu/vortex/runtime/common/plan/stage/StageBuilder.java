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
package edu.snu.vortex.runtime.common.plan.stage;

import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.common.dag.DAGBuilder;

import java.util.List;

/**
 * Stage Builder.
 */
public final class StageBuilder {
  private final DAGBuilder<IRVertex, IREdge> stageInternalDAGBuilder;
  private final Integer stageId;

  /**
   * Builds a {@link Stage}.
   * @param stageId stage ID in numeric form.
   */
  public StageBuilder(final Integer stageId) {
    this.stageId = stageId;
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
   * Integrity check for stages.
   * @param stage stage to check for.
   */
  private void integrityCheck(final Stage stage) {
    final List<IRVertex> vertices = stage.getStageInternalDAG().getVertices();

    final Attribute firstPlacement = vertices.iterator().next().getAttr(Attribute.Key.Placement);
    vertices.forEach(irVertex -> {
      if (!irVertex.getAttr(Attribute.Key.Placement).equals(firstPlacement)) {
        throw new RuntimeException("Vertices of the same stage have different attributes: " + irVertex.getId());
      }
    });
  }

  /**
   * Builds and returns the {@link Stage}.
   * @return the runtime stage.
   */
  public Stage build() {
    final Stage stage = new Stage(RuntimeIdGenerator.generateStageId(stageId),
        stageInternalDAGBuilder.buildWithoutSourceSinkCheck());
    integrityCheck(stage);
    return stage;
  }
}
