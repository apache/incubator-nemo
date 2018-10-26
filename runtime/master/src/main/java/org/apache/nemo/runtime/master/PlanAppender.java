/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.master;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.IdManager;
import org.apache.nemo.common.ir.edge.executionproperty.CacheIDProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DuplicateEdgeGroupPropertyValue;
import org.apache.nemo.common.ir.vertex.CachedSourceVertex;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.IgnoreSchedulingTempDataReceiverProperty;
import org.apache.nemo.runtime.common.exception.PlanAppenderException;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.Stage;
import org.apache.nemo.runtime.common.plan.StageEdge;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * An implementation of plan appender which append two plans regarding to caching.
 */
public final class PlanAppender {

  /**
   * Private constructor.
   */
  private PlanAppender() {
    // Private constructor.
  }

  /**
   * Append to plans regarding to caching.
   * For more information about caching part, check {@link IgnoreSchedulingTempDataReceiverProperty}.
   *
   * @param originalPlan the original plan.
   * @param planToAppend the plan to append.
   * @return the appended plan.
   */
  public static PhysicalPlan appendPlan(final PhysicalPlan originalPlan,
                                        final PhysicalPlan planToAppend) {
    // Add the stage DAG of the original plan to the builder at first.
    final DAGBuilder<Stage, StageEdge> physicalDAGBuilder = new DAGBuilder<>(originalPlan.getStageDAG());

    // Scan cached data in the original plan.
    final Map<UUID, StageEdge> cachedEdges = new HashMap<>();
    originalPlan.getStageDAG().getVertices().forEach(
      stage -> originalPlan.getStageDAG().getIncomingEdgesOf(stage).stream()
        // Cached edge toward a temporary data receiver is a representative edge.
        .filter(stageEdge ->
          stageEdge.getDstIRVertex().getPropertyValue(IgnoreSchedulingTempDataReceiverProperty.class).isPresent())
        .forEach(stageEdge -> stageEdge.getPropertyValue(CacheIDProperty.class)
          .ifPresent(cacheId -> cachedEdges.put(cacheId, stageEdge))
        ));

    // Scan CacheID to a pair of cached source vertex and it's stage from the plan to append.
    final Map<UUID, Pair<IRVertex, Stage>> cacheCandidates = new HashMap<>();
    final DAG<Stage, StageEdge> dagToAppend = planToAppend.getStageDAG();
    dagToAppend.topologicalDo(stage -> {
      // Add the stage DAG of the plan to append to the builder.
      physicalDAGBuilder.addVertex(stage);
      dagToAppend.getIncomingEdgesOf(stage).
        forEach(edge -> {
          physicalDAGBuilder.connectVertices(edge);
          // Find cached-data requiring stage edges in the submitted plan.
          if (edge.getSrcIRVertex() instanceof CachedSourceVertex) {
            final UUID cacheId = edge.getPropertyValue(CacheIDProperty.class)
              .orElseThrow(() -> new PlanAppenderException("No cache id in the cached edge " + edge.getId()));
            cacheCandidates.put(cacheId, Pair.of(edge.getSrcIRVertex(), edge.getSrc()));
          }
        });

      // Find cached-data requiring ir edges in the submitted plan.
      final DAG<IRVertex, RuntimeEdge<IRVertex>> stageIRDAG = stage.getIRDAG();
      stageIRDAG.getVertices().stream()
        .filter(irVertex -> irVertex instanceof CachedSourceVertex)
        .forEach(cachedSourceVertex ->
          stageIRDAG.getOutgoingEdgesOf(cachedSourceVertex).forEach(runtimeEdge -> {
            final UUID cacheId = runtimeEdge.getPropertyValue(CacheIDProperty.class)
              .orElseThrow(
                () -> new PlanAppenderException("No cache id in the cached edge " + runtimeEdge.getId()));
            cacheCandidates.put(cacheId, Pair.of(runtimeEdge.getSrc(), stage));
          }));
    });

    // Link the cached data and the stages require the data.
    cacheCandidates.forEach((cacheId, vertexStagePair) -> {
      final StageEdge cachedEdge = cachedEdges.get(cacheId);
      if (cachedEdge != null) {
        final StageEdge newEdge = new StageEdge(
          IdManager.newEdgeId(),
          cachedEdge.getExecutionProperties(),
          cachedEdge.getSrcIRVertex(),
          vertexStagePair.left(),
          cachedEdge.getSrc(),
          vertexStagePair.right());
        physicalDAGBuilder.connectVertices(newEdge);
        final DuplicateEdgeGroupPropertyValue duplicateEdgeGroupPropertyValue =
          cachedEdge.getPropertyValue(DuplicateEdgeGroupProperty.class)
            .orElseThrow(() -> new PlanAppenderException("Cached edge does not have duplicated edge group property."));
        duplicateEdgeGroupPropertyValue.setGroupSize(duplicateEdgeGroupPropertyValue.getGroupSize() + 1);
        newEdge.getExecutionProperties().put(DuplicateEdgeGroupProperty.of(duplicateEdgeGroupPropertyValue));
      } else {
        throw new PlanAppenderException("Cached edge is not found in the original plan.");
      }
    });

    return new PhysicalPlan(originalPlan.getPlanId(), physicalDAGBuilder.build());
  }
}
