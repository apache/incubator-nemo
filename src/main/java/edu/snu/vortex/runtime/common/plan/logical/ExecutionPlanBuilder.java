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

import edu.snu.vortex.compiler.frontend.beam.BoundedSourceVertex;
import edu.snu.vortex.compiler.ir.*;
import edu.snu.vortex.runtime.common.*;
import edu.snu.vortex.runtime.exception.IllegalVertexOperationException;
import edu.snu.vortex.runtime.utils.RuntimeAttributeConverter;

import java.util.*;

/**
 * ExecutionPlanBuilder.
 */
public final class ExecutionPlanBuilder {
  private final List<RuntimeStageBuilder> runtimeStageBuilderList;
  private final Map<String, RuntimeStageBuilder> vertexIdToRuntimeStageBuilderMap;
  private RuntimeStageBuilder stageBuilder;

  /**
   * Builds an {@link ExecutionPlan}.
   */
  public ExecutionPlanBuilder() {
    this.runtimeStageBuilderList = new LinkedList<>();
    this.vertexIdToRuntimeStageBuilderMap = new HashMap<>();
    this.stageBuilder = new RuntimeStageBuilder();
  }

  /**
   * Adds a {@link Vertex} to the execution plan, by adding it to the stage currently being built.
   * The vertices must be added in the order of execution.
   * @param vertex to add.
   */
  public void addVertex(final Vertex vertex) {
    final RuntimeVertex newVertex;

    // TODO #100: Add Vertex Type in IR
    if (vertex instanceof BoundedSourceVertex) {
      newVertex = new RuntimeBoundedSourceVertex((BoundedSourceVertex) vertex,
          RuntimeAttributeConverter.convertVertexAttributes(vertex.getAttributes()));
    } else if (vertex instanceof OperatorVertex) {
      newVertex = new RuntimeOperatorVertex((OperatorVertex) vertex,
          RuntimeAttributeConverter.convertVertexAttributes(vertex.getAttributes()));
    } else {
      throw new IllegalVertexOperationException("Supported types: BoundedSourceVertex, OperatorVertex");
    }
    stageBuilder.addRuntimeVertex(newVertex);
    vertexIdToRuntimeStageBuilderMap.putIfAbsent(newVertex.getId(), stageBuilder);
  }

  public void connectVertices(final Edge edge) {
    final String srcRuntimeVertexId = RuntimeIdGenerator.generateRuntimeVertexId(edge.getSrc().getId());
    final String dstRuntimeVertexId = RuntimeIdGenerator.generateRuntimeVertexId(edge.getDst().getId());

    final RuntimeStageBuilder srcRuntimeStageBuilder = vertexIdToRuntimeStageBuilderMap.get(srcRuntimeVertexId);
    final RuntimeStageBuilder dstRuntimeStageBuilder = vertexIdToRuntimeStageBuilderMap.get(dstRuntimeVertexId);

    if (srcRuntimeStageBuilder == null || dstRuntimeStageBuilder == null) {
      throw new IllegalVertexOperationException(
          "srcRuntimeVertex and/or dstRuntimeVertex are not yet added to the ExecutionPlanBuilder");
    }

    if (srcRuntimeStageBuilder.equals(dstRuntimeStageBuilder)) {
      connectStageInternalVertices(srcRuntimeVertexId, dstRuntimeVertexId);
    } else {
      connectStageBoundaryVertices(srcRuntimeStageBuilder, dstRuntimeStageBuilder,
          srcRuntimeVertexId, dstRuntimeVertexId, edge);
    }
  }

  /**
   * Connects two {@link RuntimeVertex} that belong to the same stage, using the information given in {@link Edge}.
   * @param srcRuntimeVertexId source vertex id.
   * @param dstRuntimeVertexId destination vertex id.
   */
  private void connectStageInternalVertices(final String srcRuntimeVertexId, final String dstRuntimeVertexId) {
    stageBuilder.connectInternalRuntimeVertices(srcRuntimeVertexId, dstRuntimeVertexId);
  }

  /**
   * Connects two {@link RuntimeVertex} that belong to different stages, using the information given in {@link Edge}.
   * @param srcStageBuilder the source stage builder.
   * @param dstStageBuilder the destination stage builder.
   * @param srcRuntimeVertexId source vertex id.
   * @param dstRuntimeVertexId destination vertex id.
   * @param edge to use for the connection.
   */
  private void connectStageBoundaryVertices(final RuntimeStageBuilder srcStageBuilder,
                                            final RuntimeStageBuilder dstStageBuilder,
                                            final String srcRuntimeVertexId,
                                            final String dstRuntimeVertexId,
                                            final Edge edge) {
    final RuntimeVertex srcRuntimeVertex = srcStageBuilder.getRuntimeVertexById(srcRuntimeVertexId);
    final RuntimeVertex dstRuntimeVertex = dstStageBuilder.getRuntimeVertexById(dstRuntimeVertexId);

    if (srcRuntimeVertex == null || dstRuntimeVertex == null) {
      throw new IllegalVertexOperationException("unable to locate srcRuntimeVertex and/or dstRuntimeVertex");
    }

    final RuntimeEdge newEdge = new RuntimeEdge(edge.getId(),
        RuntimeAttributeConverter.convertEdgeAttributes(edge.getAttributes()),
        srcRuntimeVertex, dstRuntimeVertex);

    srcStageBuilder.connectRuntimeStages(srcRuntimeVertex, newEdge);
    dstStageBuilder.connectRuntimeStages(dstRuntimeVertex, newEdge);
  }

  /**
   * Creates and adds a new {@link RuntimeStage} to the execution plan.
   * The {@link RuntimeStage} that was previously created is finalized.
   * Stages must be created in the order of execution.
   */
  public void createNewStage() {
    addCurrentStage();
    stageBuilder = new RuntimeStageBuilder();
  }

  /**
   * Builds and returns the {@link ExecutionPlan} to be submitted to Runtime.
   * @return the execution plan.
   */
  public ExecutionPlan build() {
    addCurrentStage();
    return new ExecutionPlan(RuntimeIdGenerator.generateExecutionPlanId(), runtimeStageBuilderList);
  }

  /**
   *   Adds the current stage being built if the stage is valid.
   */
  private void addCurrentStage() {
    if (!stageBuilder.isEmpty()) {
      runtimeStageBuilderList.add(stageBuilder);
    }
  }
}
