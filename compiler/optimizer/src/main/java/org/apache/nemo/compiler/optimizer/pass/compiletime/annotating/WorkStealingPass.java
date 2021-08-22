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
package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.EnableWorkStealingProperty;
import org.apache.nemo.common.ir.vertex.transform.Transform;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;
import org.apache.nemo.runtime.common.plan.StagePartitioner;

import java.util.*;

/**
 * Optimization pass for tagging enable work stealing execution property.
 */
@Annotates(EnableWorkStealingProperty.class)
@Requires(CommunicationPatternProperty.class)
public final class WorkStealingPass extends AnnotatingPass {
  private static final String SPLIT_STRATEGY = "SPLIT";
  private static final String MERGE_STRATEGY = "MERGE";
  private static final String DEFAULT_STRATEGY = "DEFAULT";

  private final StagePartitioner stagePartitioner = new StagePartitioner();

  public WorkStealingPass() {
    super(WorkStealingPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG irdag) {
    irdag.topologicalDo(irVertex -> {
      final boolean notConnectedToO2OEdge = irdag.getIncomingEdgesOf(irVertex).stream()
        .map(edge -> edge.getPropertyValue(CommunicationPatternProperty.class).get())
        .noneMatch(property -> property.equals(CommunicationPatternProperty.Value.ONE_TO_ONE));
      if (irVertex instanceof OperatorVertex && notConnectedToO2OEdge) {
        Transform transform = ((OperatorVertex) irVertex).getTransform();
        if (transform.toString().contains("work stealing")) {
          irVertex.setProperty(EnableWorkStealingProperty.of(SPLIT_STRATEGY));
        } else if (transform.toString().contains("merge")) {
          irVertex.setProperty(EnableWorkStealingProperty.of(MERGE_STRATEGY));
        } else {
          irVertex.setProperty(EnableWorkStealingProperty.of(DEFAULT_STRATEGY));
        }
      } else {
        irVertex.setProperty(EnableWorkStealingProperty.of(DEFAULT_STRATEGY));
      }
    });
    return tidyWorkStealingAnnotation(irdag);
  }


  /**
   * Tidy annotated dag.
   * Cleanup conditions:
   * -  The number of SPLIT annotations and MERGE annotations should be equal
   * -  SPLIT and MERGE should not be together in one stage, but needs to be in adjacent stage.
   * -  For now, nested work stealing optimizations are not provided. If detected, leave only the
   *    innermost pair.
   *
   * @param irdag   irdag to cleanup.
   * @return        cleaned irdag.
   */
  private IRDAG tidyWorkStealingAnnotation(final IRDAG irdag) {
    String splitVertexId = null;

    final List<Pair<String, String>> splitMergePairs = new ArrayList<>();
    final Set<String> pairedVertices = new HashSet<>();
    final Map<Integer, Set<IRVertex>> stageIdToStageVertices = new HashMap<>();

    // Make SPLIT - MERGE vertex pair.
    for (IRVertex vertex : irdag.getTopologicalSort()) {
      if (vertex.getPropertyValue(EnableWorkStealingProperty.class).get().equals(SPLIT_STRATEGY)) {
        if (splitVertexId != null) {
          // nested SPLIT vertex detected: delete the prior one.
          irdag.getVertexById(splitVertexId).setProperty(EnableWorkStealingProperty.of(DEFAULT_STRATEGY));
        }
        splitVertexId = vertex.getId();
      } else if (vertex.getPropertyValue(EnableWorkStealingProperty.class).get().equals(MERGE_STRATEGY)) {
        if (splitVertexId != null) {
          splitMergePairs.add(Pair.of(splitVertexId, vertex.getId()));
          pairedVertices.add(splitVertexId);
          pairedVertices.add(vertex.getId());
          splitVertexId = null;
        } else {
          // no corresponding SPLIT vertex: delete
          vertex.setProperty(EnableWorkStealingProperty.of(DEFAULT_STRATEGY));
        }
      }
    }

    final Map<IRVertex, Integer> vertexToStageId = stagePartitioner.apply(irdag);

    for (Pair<String, String> splitMergePair : splitMergePairs) {
      IRVertex splitVertex = irdag.getVertexById(splitMergePair.left());
      IRVertex mergeVertex = irdag.getVertexById(splitMergePair.right());

      if (vertexToStageId.get(splitVertex) >= vertexToStageId.get(mergeVertex)
        || irdag.getIncomingEdgesOf(mergeVertex).stream()
        .map(Edge::getSrc)
        .map(vertexToStageId::get)
        .noneMatch(stageId -> stageId.equals(vertexToStageId.get(splitVertex)))) {
        // split vertex is descendent of merge vertex or they are in the same stage,
        // or they are not in adjacent stages
        splitVertex.setProperty(EnableWorkStealingProperty.of(DEFAULT_STRATEGY));
        mergeVertex.setProperty(EnableWorkStealingProperty.of(DEFAULT_STRATEGY));
        pairedVertices.remove(splitVertex.getId());
        pairedVertices.remove(mergeVertex.getId());
      }
    }

    irdag.topologicalDo(vertex -> {
      if (!vertex.getPropertyValue(EnableWorkStealingProperty.class)
        .orElse(DEFAULT_STRATEGY).equals(DEFAULT_STRATEGY)) {
        if (!pairedVertices.contains(vertex.getId())) {
          vertex.setProperty(EnableWorkStealingProperty.of(DEFAULT_STRATEGY));
        }
      }
    });

    // update execution property of other vertices in same stage.
    vertexToStageId.forEach((vertex, stageId) -> {
      if (!stageIdToStageVertices.containsKey(stageId)) {
        stageIdToStageVertices.put(stageId, new HashSet<>());
      }
      stageIdToStageVertices.get(stageId).add(vertex);
    });

    for (String vertexId : pairedVertices) {
      IRVertex vertex = irdag.getVertexById(vertexId);
      Set<IRVertex> stageVertices = stageIdToStageVertices.get(vertexToStageId.get(vertex));
      String strategy = vertex.getPropertyValue(EnableWorkStealingProperty.class)
        .orElse(DEFAULT_STRATEGY);
      for (IRVertex stageVertex : stageVertices) {
        stageVertex.setProperty(EnableWorkStealingProperty.of(strategy));
      }
    }

    return irdag;
  }
}
