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
package edu.snu.vortex.runtime.common.plan.physical;

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.common.dag.DAGBuilder;
import edu.snu.vortex.compiler.frontend.beam.BoundedSourceVertex;
import edu.snu.vortex.compiler.ir.*;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.compiler.ir.attribute.AttributeMap;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.common.plan.stage.*;
import edu.snu.vortex.runtime.exception.IllegalVertexOperationException;
import edu.snu.vortex.runtime.exception.PhysicalPlanGenerationException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static edu.snu.vortex.compiler.ir.attribute.Attribute.Memory;

/**
 * A function that converts an IR DAG to physical DAG.
 */
public final class PhysicalPlanGenerator
    implements Function<DAG<IRVertex, IREdge>, DAG<PhysicalStage, PhysicalStageEdge>> {

  final String dagDirectory;

  @Inject
  private PhysicalPlanGenerator(@Parameter(JobConf.DAGDirectory.class) final String dagDirectory) {
    this.dagDirectory = dagDirectory;
  }

  /**
   * Generates the {@link PhysicalPlan} to be executed.
   * @param irDAG that should be converted to a physical execution plan
   * @return {@link PhysicalPlan} to execute.
   */
  @Override
  public DAG<PhysicalStage, PhysicalStageEdge> apply(final DAG<IRVertex, IREdge> irDAG) {
    // first, stage-partition the IR DAG.
    final DAG<Stage, StageEdge> dagOfStages = stagePartitionIrDAG(irDAG);
    // for debugging purposes.
    dagOfStages.storeJSON(dagDirectory, "plan-logical", "logical execution plan");
    // then create tasks and make it into a physical execution plan.
    return stagesIntoPlan(dagOfStages);
  }

  /**
   * Partitions an IR DAG into stages.
   * We traverse the DAG topologically to observe each vertex if it can be added to a stage or if it should be assigned
   * to a new stage. We filter out the candidate incoming edges to connect to an existing stage, and if it exists, we
   * connect it to the stage, and otherwise we don't.
   * @param irDAG to partition.
   * @return the partitioned DAG, composed of stages and stage edges.
   */
  public DAG<Stage, StageEdge> stagePartitionIrDAG(final DAG<IRVertex, IREdge> irDAG) {
    final DAGBuilder<Stage, StageEdge> dagOfStagesBuilder = new DAGBuilder<>();

    final List<List<IRVertex>> vertexListForEachStage = groupVerticesByStage(irDAG);

    final Map<IRVertex, Stage> vertexStageMap = new HashMap<>();

    for (final List<IRVertex> stageVertices : vertexListForEachStage) {
      final Set<IRVertex> currentStageVertices = new HashSet<>();
      final Set<StageEdgeBuilder> currentStageIncomingEdges = new HashSet<>();

      // Create a new stage builder.
      final StageBuilder stageBuilder = new StageBuilder();

      // For each vertex in the stage,
      for (final IRVertex irVertex : stageVertices) {

        // Add vertex to the stage.
        stageBuilder.addVertex(irVertex);
        currentStageVertices.add(irVertex);

        // Connect all the incoming edges for the vertex
        final List<IREdge> inEdges = irDAG.getIncomingEdgesOf(irVertex);
        final Optional<List<IREdge>> inEdgeList = (inEdges == null) ? Optional.empty() : Optional.of(inEdges);
        inEdgeList.ifPresent(edges -> edges.forEach(irEdge -> {
          final IRVertex srcVertex = irEdge.getSrc();
          final IRVertex dstVertex = irEdge.getDst();

          if (srcVertex == null) {
            throw new IllegalVertexOperationException("Unable to locate srcVertex for IREdge " + irEdge);
          } else if (dstVertex == null) {
            throw new IllegalVertexOperationException("Unable to locate dstVertex for IREdge " + irEdge);
          }

          // both vertices are in the stage.
          if (currentStageVertices.contains(srcVertex) && currentStageVertices.contains(dstVertex)) {
            stageBuilder.connectInternalVertices(irEdge);
          } else { // edge comes from another stage
            final Stage srcStage = vertexStageMap.get(srcVertex);

            if (srcStage == null) {
              throw new IllegalVertexOperationException("srcVertex " + srcVertex + " not yet added to the builder");
            }

            final StageEdgeBuilder newEdgeBuilder = new StageEdgeBuilder(irEdge.getId())
                .setEdgeAttributes(irEdge.getAttributes())
                .setSrcVertex(srcVertex).setDstVertex(dstVertex)
                .setSrcStage(srcStage)
                .setCoder(irEdge.getCoder());
            currentStageIncomingEdges.add(newEdgeBuilder);
          }
        }));
      }

      // If this runtime stage contains at least one vertex, build it!
      if (!stageBuilder.isEmpty()) {
        final Stage currentStage = stageBuilder.build();
        dagOfStagesBuilder.addVertex(currentStage);

        // Add this stage as the destination stage for all the incoming edges.
        currentStageIncomingEdges.forEach(stageEdgeBuilder -> {
          stageEdgeBuilder.setDstStage(currentStage);
          final StageEdge stageEdge = stageEdgeBuilder.build();
          dagOfStagesBuilder.connectVertices(stageEdge);
        });
        currentStageIncomingEdges.clear();

        currentStageVertices.forEach(irVertex -> vertexStageMap.put(irVertex, currentStage));
        currentStageVertices.clear();
      }
    }

    return dagOfStagesBuilder.build();
  }

  /**
   * This method traverses the IR DAG to group each of the vertices by stages.
   * @param irDAG to traverse.
   * @return List of groups of vertices that are each divided by stages.
   */
  private List<List<IRVertex>> groupVerticesByStage(final DAG<IRVertex, IREdge> irDAG) {
    // Data structures used for stage partitioning.
    final HashMap<IRVertex, Integer> vertexStageNumHashMap = new HashMap<>();
    final List<List<IRVertex>> vertexListForEachStage = new ArrayList<>();
    final AtomicInteger stageNumber = new AtomicInteger(0);
    final List<Integer> dependentStagesList = new ArrayList<>();

    // First, traverse the DAG topologically to add each vertices to a list associated with each of the stage number.
    irDAG.topologicalDo(vertex -> {
      final List<IREdge> inEdges = irDAG.getIncomingEdgesOf(vertex);
      final Optional<List<IREdge>> inEdgeList = (inEdges == null || inEdges.isEmpty())
          ? Optional.empty() : Optional.of(inEdges);

      if (!inEdgeList.isPresent()) { // If Source vertex
        createNewStage(vertex, vertexStageNumHashMap, stageNumber, vertexListForEachStage);
      } else {
        // Filter candidate incoming edges that can be included in a stage with the vertex.
        final Optional<List<IREdge>> inEdgesForStage = inEdgeList.map(e -> e.stream()
            .filter(edge -> edge.getType().equals(IREdge.Type.OneToOne)) // One to one edges
            .filter(edge -> edge.getAttr(Attribute.Key.ChannelDataPlacement).equals(Memory)) // Memory data placement
            .filter(edge -> edge.getSrc().getAttr(Attribute.Key.Placement)
                .equals(edge.getDst().getAttr(Attribute.Key.Placement))) //Src and Dst same placement
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
          createNewStage(vertex, vertexStageNumHashMap, stageNumber, vertexListForEachStage);
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
    return vertexListForEachStage;
  }

  /**
   * Creates a new stage.
   * @param irVertex the vertex which begins the stage.
   * @param vertexStageNumHashMap to keep track of vertex and its stage number.
   * @param stageNumber to atomically number stages.
   * @param vertexListForEachStage to group each vertex lists for each stages.
   */
  private static void createNewStage(final IRVertex irVertex, final HashMap<IRVertex, Integer> vertexStageNumHashMap,
                                     final AtomicInteger stageNumber,
                                     final List<List<IRVertex>> vertexListForEachStage) {
    vertexStageNumHashMap.put(irVertex, stageNumber.getAndIncrement());
    final List<IRVertex> newList = new ArrayList<>();
    newList.add(irVertex);
    vertexListForEachStage.add(newList);
  }

  // Map that keeps track of the IRVertex of each tasks
  private final Map<Task, IRVertex> taskIRVertexMap = new HashMap<>();
  /**
   * Getter for taskIRVertexMap.
   * @return the taskIRVertexMap.
   */
  public Map<Task, IRVertex> getTaskIRVertexMap() {
    return taskIRVertexMap;
  }

  /**
   * Converts the given DAG of stages to a physical DAG for execution.
   * @param dagOfStages IR DAG partitioned into stages.
   * @return the converted physical DAG to execute,
   * which consists of {@link PhysicalStage} and their relationship represented by {@link PhysicalStageEdge}.
   */
  private DAG<PhysicalStage, PhysicalStageEdge> stagesIntoPlan(final DAG<Stage, StageEdge> dagOfStages) {
    final Map<String, PhysicalStage> runtimeStageIdToPhysicalStageMap = new HashMap<>();
    final DAGBuilder<PhysicalStage, PhysicalStageEdge> physicalDAGBuilder = new DAGBuilder<>();

    final Map<IRVertex, Task> irVertexTaskMap = new HashMap<>();
    for (final Stage stage : dagOfStages.getVertices()) {
      final PhysicalStageBuilder physicalStageBuilder;
      final List<IRVertex> stageVertices = stage.getStageInternalDAG().getVertices();

      final AttributeMap firstVertexAttrs = stageVertices.iterator().next().getAttributes();
      final Integer stageParallelism = firstVertexAttrs.get(Attribute.IntegerKey.Parallelism);
      final Attribute containerType = firstVertexAttrs.get(Attribute.Key.Placement);

      // Begin building a new stage in the physical plan.
      physicalStageBuilder = new PhysicalStageBuilder(stage.getId(), stageParallelism);

      // (parallelism) number of task groups will be created.
      IntStream.range(0, stageParallelism).forEach(taskGroupIndex -> {

        final DAGBuilder<Task, RuntimeEdge<Task>> stageInternalDAGBuilder = new DAGBuilder<>();

        // Iterate over the vertices contained in this stage to convert to tasks.
        stageVertices.forEach(irVertex -> {
          final Task newTaskToAdd;
          if (irVertex instanceof BoundedSourceVertex) {
            final BoundedSourceVertex sourceVertex = (BoundedSourceVertex) irVertex;

            try {
              final List<Reader> readers = sourceVertex.getReaders(stageParallelism);
              if (readers.size() != stageParallelism) {
                throw new RuntimeException("Actual parallelism differs from the one specified by IR: "
                    + readers.size() + " and " + stageParallelism);
              }
              newTaskToAdd = new BoundedSourceTask<>(RuntimeIdGenerator.generateTaskId(), sourceVertex.getId(),
                  taskGroupIndex, readers.get(taskGroupIndex));
            } catch (Exception e) {
              throw new PhysicalPlanGenerationException(e);
            }
          } else if (irVertex instanceof OperatorVertex) {
            final OperatorVertex operatorVertex = (OperatorVertex) irVertex;
            newTaskToAdd = new OperatorTask(RuntimeIdGenerator.generateTaskId(), operatorVertex.getId(),
                taskGroupIndex, operatorVertex.getTransform());

          } else if (irVertex instanceof MetricCollectionBarrierVertex) {
            final MetricCollectionBarrierVertex metricCollectionBarrierVertex =
                (MetricCollectionBarrierVertex) irVertex;
            newTaskToAdd = new MetricCollectionBarrierTask(RuntimeIdGenerator.generateTaskId(),
                metricCollectionBarrierVertex.getId(), taskGroupIndex);
          } else {
            throw new IllegalVertexOperationException("This vertex type is not supported");
          }
          stageInternalDAGBuilder.addVertex(newTaskToAdd);
          irVertexTaskMap.put(irVertex, newTaskToAdd);
          taskIRVertexMap.put(newTaskToAdd, irVertex);
        });

        // connect internal edges in the task group. It suffices to iterate over only the stage internal inEdges.
        final DAG<IRVertex, IREdge> stageInternalDAG = stage.getStageInternalDAG();
        stageInternalDAG.getVertices().forEach(irVertex -> {
          final List<IREdge> inEdges = stageInternalDAG.getIncomingEdgesOf(irVertex);
          inEdges.forEach(edge ->
              stageInternalDAGBuilder.connectVertices(new RuntimeEdge<>(edge.getId(), edge.getAttributes(),
                  irVertexTaskMap.get(edge.getSrc()), irVertexTaskMap.get(edge.getDst()), edge.getCoder())));
        });

        // Create the task group to add for this stage.
        final TaskGroup newTaskGroup = new TaskGroup(RuntimeIdGenerator.generateTaskGroupId(), stage.getId(),
            taskGroupIndex, stageInternalDAGBuilder.build(), containerType);
        physicalStageBuilder.addTaskGroup(newTaskGroup);
        irVertexTaskMap.clear();
      });
      final PhysicalStage physicalStage = physicalStageBuilder.build();
      physicalDAGBuilder.addVertex(physicalStage);

      runtimeStageIdToPhysicalStageMap.put(stage.getId(), physicalStage);
    }

    // Connect Physical stages
    dagOfStages.getVertices().forEach(stage ->
      dagOfStages.getIncomingEdgesOf(stage).forEach(stageEdge -> {
        final PhysicalStage srcStage = runtimeStageIdToPhysicalStageMap.get(stageEdge.getSrc().getId());
        final PhysicalStage dstStage = runtimeStageIdToPhysicalStageMap.get(stageEdge.getDst().getId());

        physicalDAGBuilder.connectVertices(new PhysicalStageEdge(stageEdge.getId(),
            stageEdge.getAttributes(),
            stageEdge.getSrcVertex(), stageEdge.getDstVertex(),
            srcStage, dstStage,
            stageEdge.getCoder()));
      }));

    return physicalDAGBuilder.build();
  }
}
