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

import java.util.*;

/**
 * Represents a stage in Runtime's execution of a job.
 * Each stage contains a part of a whole execution plan.
 * Stage partitioning is determined by {@link edu.snu.vortex.compiler.backend.vortex.VortexBackend}.
 */
public final class RuntimeStage {
  private final String stageId;

  /**
   * The list of {@link RuntimeVertex} to execute, sorted in the order of execution.
   */
  private final List<RuntimeVertex> runtimeVertices;

  /**
   * {@link RuntimeVertex}'s id to the set of local incoming edges from the vertices in this stage.
   */
  private final Map<String, Set<String>> internalInEdges;

  /**
   * {@link RuntimeVertex}'s id to the set of local outgoing edges to the vertices in this stage.
   */
  private final Map<String, Set<String>> internalOutEdges;

  /**
   * {@link RuntimeVertex}'s id to the set of remote incoming edges from the vertices in other stages.
   */
  private final Map<String, Set<RuntimeEdge>> stageIncomingEdges;

  /**
   * {@link RuntimeVertex}'s id to the set of remote outgoing edges to the vertices in other stages.
   */
  private final Map<String, Set<RuntimeEdge>> stageOutgoingEdges;

  public RuntimeStage(final String stageId,
                      final List<RuntimeVertex> runtimeVertices,
                      final Map<String, Set<String>> internalInEdges,
                      final Map<String, Set<String>> internalOutEdges,
                      final Map<String, Set<RuntimeEdge>> stageIncomingEdges,
                      final Map<String, Set<RuntimeEdge>> stageOutgoingEdges) {
    this.stageId = stageId;
    this.runtimeVertices = runtimeVertices;
    this.internalInEdges = internalInEdges;
    this.internalOutEdges = internalOutEdges;
    this.stageIncomingEdges = stageIncomingEdges;
    this.stageOutgoingEdges = stageOutgoingEdges;
  }

  public String getStageId() {
    return stageId;
  }

  public List<RuntimeVertex> getRuntimeVertices() {
    return runtimeVertices;
  }

  public Map<String, Set<String>> getInternalInEdges() {
    return internalInEdges;
  }

  public Map<String, Set<String>> getInternalOutEdges() {
    return internalOutEdges;
  }

  public Map<String, Set<RuntimeEdge>> getStageIncomingEdges() {
    return stageIncomingEdges;
  }

  public Map<String, Set<RuntimeEdge>> getStageOutgoingEdges() {
    return stageOutgoingEdges;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("RuntimeStage{");
    sb.append("stageId='");
    sb.append(stageId);
    sb.append("\', runtimeVertices=");
    sb.append(runtimeVertices);
    sb.append('}');
    return sb.toString();
  }
}
