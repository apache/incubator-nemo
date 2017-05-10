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
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.OperatorVertex;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.exception.IllegalVertexOperationException;
import edu.snu.vortex.runtime.utils.RuntimeAttributeConverter;
import edu.snu.vortex.utils.dag.DAG;
import edu.snu.vortex.utils.dag.DAGBuilder;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static edu.snu.vortex.compiler.ir.attribute.Attribute.Local;

/**
 * A function that converts an IR DAG to runtime's logical DAG.
 * Designed to be called in {@link edu.snu.vortex.compiler.backend.vortex.VortexBackend}.
 */
public final class LogicalDAGGenerator
    implements Function<DAG<IRVertex, IREdge>, DAG<Stage, StageEdge>> {

  /**
   * The IR DAG to convert.
   */
  private DAG<IRVertex, IREdge> irDAG;

  /**
   * The builder for the logical DAG.
   */
  private final DAGBuilder<Stage, StageEdge> logicalDAGBuilder;

  /**
   * Data structures used for stage partitioning in Vortex Backend.
   */
  private final HashMap<IRVertex, Integer> vertexStageNumHashMap;
  private final List<List<IRVertex>> vertexListForEachStage;
  private final AtomicInteger stageNumber;

  public LogicalDAGGenerator() {
    logicalDAGBuilder = new DAGBuilder<>();
    vertexStageNumHashMap = new HashMap<>();
    vertexListForEachStage = new ArrayList<>();
    stageNumber = new AtomicInteger(0);
  }

  /**
   * Converts the given IR DAG to its Runtime representation, a logical DAG.
   * @param inputDAG the optimized IR DAG to be submitted to Runtime after the conversion.
   * @return the converted logical DAG to submit to Runtime,
   * which consists of {@link Stage} and their relationship represented by
   * {@link StageEdge}.
   */
  @Override
  public DAG<Stage, StageEdge> apply(final DAG<IRVertex, IREdge> inputDAG) {
    this.irDAG = inputDAG;

    stagePartitionIrDAG();
    convertToLogicalDAG();

    return logicalDAGBuilder.build();
  }

  /**
   * Partitions an IR DAG into stages. Prepares for IR -> logical DAG conversion.
   * We traverse the DAG topologically to observe each vertex if it can be added to a stage or if it should be assigned
   * to a new stage. We filter out the candidate incoming edges to connect to an existing stage, and if it exists, we
   * connect it to the stage, and otherwise we don't.
   */
  private void stagePartitionIrDAG() {
    final List<Integer> dependentStagesList = new ArrayList<>();

    // First, traverse the DAG topologically to add each vertices to a list associated with each of the stage number.
    irDAG.topologicalDo(vertex -> {
      final List<IREdge> inEdges = irDAG.getIncomingEdgesOf(vertex);
      final Optional<List<IREdge>> inEdgeList = (inEdges == null || inEdges.isEmpty()) ?
          Optional.empty() : Optional.of(inEdges);

      if (!inEdgeList.isPresent()) { // If Source vertex
        createNewStage(vertex);
      } else {
        // Filter candidate incoming edges that can be included in a stage with the vertex.
        final Optional<List<IREdge>> inEdgesForStage = inEdgeList.map(e -> e.stream()
            .filter(edge -> edge.getType().equals(IREdge.Type.OneToOne)) // One to one edges
            .filter(edge -> edge.getAttr(Attribute.Key.ChannelDataPlacement).equals(Local)) // Local data placement
            .filter(edge -> edge.getSrc().getAttributes().equals(edge.getDst().getAttributes())) //Src and Dst same attr
            .filter(edge -> vertexStageNumHashMap.containsKey(edge.getSrc())) // Src that is already included in a stage
            // Others don't depend on the candidate stage.
            .filter(edge -> !dependentStagesList.contains(vertexStageNumHashMap.get(edge.getSrc())))
            .collect(Collectors.toList()));
        // Choose one to connect out of the candidates. We want to connect the vertex to a single stage.
        final Optional<IREdge> edgeToConnect = inEdgesForStage.map(edges -> edges.stream().findAny())
            .orElse(Optional.empty());

        // Mark stages that other stages depend on
        inEdgeList.ifPresent(edges -> edges.stream()
            .filter(e -> !e.equals(edgeToConnect.orElse(null))) // e never equals null
            .forEach(inEdge -> dependentStagesList.add(vertexStageNumHashMap.get(inEdge.getSrc()))));

        if (!inEdgesForStage.isPresent() || inEdgesForStage.get().isEmpty() || !edgeToConnect.isPresent()) {
          // when we cannot connect vertex in other stages
          createNewStage(vertex);
        } else {
          // otherwise connect with a stage.
          final IRVertex irVertexToConnect = edgeToConnect.get().getSrc();
          vertexStageNumHashMap.put(vertex, vertexStageNumHashMap.get(irVertexToConnect));
          final Optional<List<IRVertex>> listOfIRVerticesOfTheStage =
              vertexListForEachStage.stream().filter(l -> l.contains(irVertexToConnect)).findFirst();
          listOfIRVerticesOfTheStage.ifPresent(lst -> {
            vertexListForEachStage.remove(lst);
            lst.add(vertex);
            vertexListForEachStage.add(lst);
          });
        }
      }
    });
  }

  /**
   * Converts an IR DAG to logical DAG given the stage partitioned DAG data.
   */
  private void convertToLogicalDAG() {
    final Set<RuntimeVertex> currentStageVertices = new HashSet<>();
    final Set<StageEdgeBuilder> currentStageIncomingEdges = new HashSet<>();
    final Map<String, RuntimeVertex> irVertexIdToRuntimeVertexMap = new HashMap<>();
    final Map<String, Stage> runtimeVertexIdToRuntimeStageMap = new HashMap<>();

    for (final List<IRVertex> stageVertices : vertexListForEachStage) {
      // Create a new runtime stage builder.
      final StageBuilder stageBuilder = new StageBuilder();

      // For each vertex in the stage,
      for (final IRVertex irVertex : stageVertices) {

        // Convert the vertex into a runtime vertex, and add to the logical DAG
        final RuntimeVertex runtimeVertex = convertVertex(irVertex);
        stageBuilder.addRuntimeVertex(runtimeVertex);
        currentStageVertices.add(runtimeVertex);
        irVertexIdToRuntimeVertexMap.put(irVertex.getId(), runtimeVertex);

        // Connect all the incoming edges for the runtime vertex
        final List<IREdge> inEdges = irDAG.getIncomingEdgesOf(irVertex);
        final Optional<List<IREdge>> inEdgeList = (inEdges == null) ? Optional.empty() : Optional.of(inEdges);
        inEdgeList.ifPresent(edges -> edges.forEach(irEdge -> {
          final RuntimeVertex srcRuntimeVertex =
              irVertexIdToRuntimeVertexMap.get(irEdge.getSrc().getId());
          final RuntimeVertex dstRuntimeVertex =
              irVertexIdToRuntimeVertexMap.get(irEdge.getDst().getId());

          if (srcRuntimeVertex == null) {
            throw new IllegalVertexOperationException("unable to locate srcRuntimeVertex for IREdge " + irEdge);
          } else if (dstRuntimeVertex == null) {
            throw new IllegalVertexOperationException("unable to locate dstRuntimeVertex for IREdge " + irEdge);
          }

          // either the edge is within the stage
          if (currentStageVertices.contains(srcRuntimeVertex) && currentStageVertices.contains(dstRuntimeVertex)) {
            stageBuilder.connectInternalRuntimeVertices(irEdge.getId(),
                RuntimeAttributeConverter.convertEdgeAttributes(irEdge.getAttributes()),
                srcRuntimeVertex, dstRuntimeVertex);

          // or the edge is from another stage
          } else {
            final Stage srcStage = runtimeVertexIdToRuntimeStageMap.get(srcRuntimeVertex.getId());

            if (srcStage == null) {
              throw new IllegalVertexOperationException(
                  "srcRuntimeVertex and/or dstRuntimeVertex are not yet added to the ExecutionPlanBuilder");
            }

            final StageEdgeBuilder newEdgeBuilder = new StageEdgeBuilder(irEdge.getId());
            newEdgeBuilder.setEdgeAttributes(RuntimeAttributeConverter.convertEdgeAttributes(irEdge.getAttributes()));
            newEdgeBuilder.setSrcRuntimeVertex(srcRuntimeVertex);
            newEdgeBuilder.setDstRuntimeVertex(dstRuntimeVertex);
            newEdgeBuilder.setSrcStage(srcStage);
            currentStageIncomingEdges.add(newEdgeBuilder);
          }
        }));
      }

      // If this runtime stage contains at least one vertex, build it!
      if (!stageBuilder.isEmpty()) {
        final Stage currentStage = stageBuilder.build();
        logicalDAGBuilder.addVertex(currentStage);

        // Add this stage as the destination stage for all the incoming edges.
        currentStageIncomingEdges.forEach(stageEdgeBuilder -> {
          stageEdgeBuilder.setDstStage(currentStage);
          final StageEdge stageEdge = stageEdgeBuilder.build();
          logicalDAGBuilder.connectVertices(stageEdge);
        });
        currentStageIncomingEdges.clear();

        currentStageVertices.forEach(runtimeVertex ->
            runtimeVertexIdToRuntimeStageMap.put(runtimeVertex.getId(), currentStage));
        currentStageVertices.clear();
      }
    }
  }

  /**
   * Converts an IR vertex into a {@link RuntimeVertex} for the logical DAG.
   * @param irVertex to convert.
   * @return the converted Runtime Vertex.
   */
  private static RuntimeVertex convertVertex(final IRVertex irVertex) {
    final RuntimeVertex newVertex;

    // TODO #100: Add irVertex Type in IR
    if (irVertex instanceof BoundedSourceVertex) {
      newVertex = new RuntimeBoundedSourceVertex((BoundedSourceVertex) irVertex,
          RuntimeAttributeConverter.convertVertexAttributes(irVertex.getAttributes()));
    } else if (irVertex instanceof OperatorVertex) {
      newVertex = new RuntimeOperatorVertex((OperatorVertex) irVertex,
          RuntimeAttributeConverter.convertVertexAttributes(irVertex.getAttributes()));
    } else {
      throw new IllegalVertexOperationException("Supported types: BoundedSourceVertex, OperatorVertex");
    }
    return newVertex;
  }

  /**
   * Creates a new stage.
   * @param irVertex the vertex which begins the stage.
   */
  private void createNewStage(final IRVertex irVertex) {
    vertexStageNumHashMap.put(irVertex, stageNumber.getAndIncrement());
    final List<IRVertex> newList = new ArrayList<>();
    newList.add(irVertex);
    vertexListForEachStage.add(newList);
  }
}
