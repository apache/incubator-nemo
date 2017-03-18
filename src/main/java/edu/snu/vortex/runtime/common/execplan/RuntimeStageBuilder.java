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

import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.exception.IllegalEdgeOperationException;
import edu.snu.vortex.runtime.exception.IllegalVertexOperationException;

import java.util.*;

/**
 * Runtime Stage Builder.
 */
public final class RuntimeStageBuilder {
  private final List<RuntimeVertex> runtimeVertices;
  private final Map<String, Set<String>> internalInEdges;
  private final Map<String, Set<String>> internalOutEdges;
  private final Map<String, Set<RuntimeEdge>> stageIncomingEdges;
  private final Map<String, Set<RuntimeEdge>> stageOutgoingEdges;

  /**
   * Builds a {@link RuntimeStage}.
   */
  public RuntimeStageBuilder() {
    this.runtimeVertices = new LinkedList<>();
    this.internalInEdges = new HashMap<>();
    this.internalOutEdges = new HashMap<>();
    this.stageIncomingEdges = new HashMap<>();
    this.stageOutgoingEdges = new HashMap<>();
  }

  /**
   * Adds a {@link RuntimeVertex} to this stage.
   * @param vertex to add.
   */
  public void addRuntimeVertex(final RuntimeVertex vertex) {
    if (runtimeVertices.contains(vertex)) {
      throw new IllegalVertexOperationException("this vertex already exists in the current stage.");
    }
    runtimeVertices.add(vertex);
  }

  /**
   * Connects two {@link RuntimeVertex} in this stage.
   * @param srcVertexId source vertex.
   * @param dstVertexId destination vertex.
   */
  public void connectInternalRuntimeVertices(final String srcVertexId,
                                             final String dstVertexId) {
    if (runtimeVertices.stream().anyMatch(vertex -> vertex.getId().equals(srcVertexId)) &&
        runtimeVertices.stream().anyMatch(vertex -> vertex.getId().equals(dstVertexId))) {
      internalInEdges.putIfAbsent(dstVertexId, new HashSet<>());
      internalInEdges.get(dstVertexId).add(srcVertexId);
      internalOutEdges.putIfAbsent(srcVertexId, new HashSet<>());
      internalOutEdges.get(srcVertexId).add(dstVertexId);
    } else {
      throw new IllegalVertexOperationException("either src or dst vertex is not a part of this stage");
    }
  }

  /**
   * Connects an external {@link RuntimeVertex} to another in this stage, using the {@link RuntimeEdge}.
   * @param endpointRuntimeVertexId of the {@link RuntimeVertex} in this stage to be connected.
   * @param connectingEdge the edge from/to the external vertex.
   */
  public void connectRuntimeStages(final String endpointRuntimeVertexId,
                                   final RuntimeEdge connectingEdge) {
    if (runtimeVertices.stream().anyMatch(vertex -> vertex.getId().equals(endpointRuntimeVertexId))) {
      if (connectingEdge.getSrcRuntimeVertexId().equals(endpointRuntimeVertexId)) {
        stageOutgoingEdges.putIfAbsent(endpointRuntimeVertexId, new HashSet<>());
        stageOutgoingEdges.get(endpointRuntimeVertexId).add(connectingEdge);
      } else if (connectingEdge.getDstRuntimeVertexId().equals(endpointRuntimeVertexId)) {
        stageIncomingEdges.putIfAbsent(endpointRuntimeVertexId, new HashSet<>());
        stageIncomingEdges.get(endpointRuntimeVertexId).add(connectingEdge);
      } else {
        throw new IllegalEdgeOperationException("this connecting edge is not applicable to this stage");
      }
    } else {
      throw new IllegalVertexOperationException("the endpoint vertex is not a part of this stage");
    }
  }

  /**
   * @return true if this builder contains any valid {@link RuntimeVertex}.
   */
  public boolean isEmpty() {
    return runtimeVertices.isEmpty();
  }

  /**
   * Builds and returns the {@link RuntimeStage}.
   * @return the runtime stage.
   */
  public RuntimeStage build() {
    return new RuntimeStage(RuntimeIdGenerator.generateRuntimeStageId(),
        runtimeVertices, internalInEdges, internalOutEdges, stageIncomingEdges, stageOutgoingEdges);
  }
}
