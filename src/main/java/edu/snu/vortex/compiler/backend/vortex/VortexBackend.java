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
package edu.snu.vortex.compiler.backend.vortex;

import edu.snu.vortex.compiler.backend.Backend;
import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.ir.Edge;
import edu.snu.vortex.compiler.ir.Vertex;
import edu.snu.vortex.runtime.common.execplan.ExecutionPlan;
import edu.snu.vortex.runtime.common.execplan.ExecutionPlanBuilder;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static edu.snu.vortex.compiler.ir.attribute.Attribute.*;

/**
 * Backend component for Vortex Runtime.
 */
public final class VortexBackend implements Backend<ExecutionPlan> {
  private final ExecutionPlanBuilder executionPlanBuilder;
  private final HashMap<Vertex, Integer> vertexStageNumHashMap;
  private final List<List<Vertex>> vertexListForEachStage;
  private static AtomicInteger stageNumber = new AtomicInteger(0);

  public VortexBackend() {
    executionPlanBuilder = new ExecutionPlanBuilder();
    vertexStageNumHashMap = new HashMap<>();
    vertexListForEachStage = new ArrayList<>();
  }

  public ExecutionPlan compile(final DAG dag) throws Exception {
    // First, traverse the DAG topologically to add each vertices to a list associated with each of the stage number.
    dag.doTopological(vertex -> {
      final Optional<List<Edge>> inEdges = dag.getInEdgesOf(vertex);

      if (!inEdges.isPresent()) { // If Source vertex
        createNewStage(vertex);
      } else {
        final Optional<List<Edge>> inEdgesForStage = inEdges.map(e -> e.stream()
            .filter(edge -> edge.getType().equals(Edge.Type.OneToOne))
            .filter(edge -> edge.getAttr(Key.EdgeChannel).equals(Memory))
            .filter(edge -> edge.getSrc().getAttributes().equals(edge.getDst().getAttributes()))
            .filter(edge -> vertexStageNumHashMap.containsKey(edge.getSrc()))
            .collect(Collectors.toList()));

        if (!inEdgesForStage.isPresent() || inEdgesForStage.get().isEmpty()) {
          // when we cannot connect vertex in other stages
          createNewStage(vertex);
        } else {
          // We consider the first edge we find. Connecting all one-to-one memory edges into a stage may create cycles.
          final Integer stageNum = vertexStageNumHashMap.get(inEdgesForStage.get().get(0).getSrc());
          vertexStageNumHashMap.put(vertex, stageNum);
          vertexListForEachStage.get(stageNum).add(vertex);
        }
      }
    });
    // Create new Stage for each vertices with distinct stages, and connect each vertices together.
    vertexListForEachStage.forEach(list -> {
      executionPlanBuilder.createNewStage();
      list.forEach(vertex -> {
        executionPlanBuilder.addVertex(vertex);
        dag.getInEdgesOf(vertex).ifPresent(edges -> edges.forEach(executionPlanBuilder::connectVertices));
      });
    });
    return executionPlanBuilder.build();
  }

  private void createNewStage(final Vertex vertex) {
    vertexStageNumHashMap.put(vertex, stageNumber.get());
    vertexListForEachStage.add(stageNumber.get(), new ArrayList<>());
    vertexListForEachStage.get(stageNumber.getAndIncrement()).add(vertex);
  }
}
