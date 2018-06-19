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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.reshaping;

import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DecoderProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.EncoderProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.LoopVertex;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;

import java.util.*;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

/**
 * Loop Optimization.
 */
public final class LoopOptimizations {
  /**
   * Private constructor.
   */
  private LoopOptimizations() {
  }

  /**
   * @return a new LoopFusionPass class.
   */
  public static LoopFusionPass getLoopFusionPass() {
    return new LoopFusionPass();
  }

  /**
   * @return a new LoopInvariantCodeMotionPass class.
   */
  public static LoopInvariantCodeMotionPass getLoopInvariantCodeMotionPass() {
    return new LoopInvariantCodeMotionPass();
  }

  /**
   * Static function to collect LoopVertices.
   * @param dag DAG to observe.
   * @param loopVertices Map to save the LoopVertices to, according to their termination conditions.
   * @param inEdges incoming edges of LoopVertices.
   * @param outEdges outgoing Edges of LoopVertices.
   * @param builder builder to build the rest of the DAG on.
   */
  private static void collectLoopVertices(final DAG<IRVertex, IREdge> dag,
                                          final List<LoopVertex> loopVertices,
                                          final Map<LoopVertex, List<IREdge>> inEdges,
                                          final Map<LoopVertex, List<IREdge>> outEdges,
                                          final DAGBuilder<IRVertex, IREdge> builder) {
    // Collect loop vertices.
    dag.topologicalDo(irVertex -> {
      if (irVertex instanceof LoopVertex) {
        final LoopVertex loopVertex = (LoopVertex) irVertex;
        loopVertices.add(loopVertex);

        dag.getIncomingEdgesOf(loopVertex).forEach(irEdge -> {
          inEdges.putIfAbsent(loopVertex, new ArrayList<>());
          inEdges.get(loopVertex).add(irEdge);
          if (irEdge.getSrc() instanceof LoopVertex) {
            final LoopVertex source = (LoopVertex) irEdge.getSrc();
            outEdges.putIfAbsent(source, new ArrayList<>());
            outEdges.get(source).add(irEdge);
          }
        });
      } else {
        builder.addVertex(irVertex, dag);
        dag.getIncomingEdgesOf(irVertex).forEach(irEdge -> {
          if (irEdge.getSrc() instanceof LoopVertex) {
            final LoopVertex loopVertex = (LoopVertex) irEdge.getSrc();
            outEdges.putIfAbsent(loopVertex, new ArrayList<>());
            outEdges.get(loopVertex).add(irEdge);
          } else {
            builder.connectVertices(irEdge);
          }
        });
      }
    });
  }

  /**
   * Pass for Loop Fusion optimization.
   */
  public static final class LoopFusionPass extends ReshapingPass {
    /**
     * Default constructor.
     */
    public LoopFusionPass() {
      super(Collections.singleton(DataCommunicationPatternProperty.class));
    }

    @Override
    public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
      final List<LoopVertex> loopVertices = new ArrayList<>();
      final Map<LoopVertex, List<IREdge>> inEdges = new HashMap<>();
      final Map<LoopVertex, List<IREdge>> outEdges = new HashMap<>();
      final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

      collectLoopVertices(dag, loopVertices, inEdges, outEdges, builder);

      // Collect and group those with same termination condition.
      final Set<Set<LoopVertex>> setOfLoopsToBeFused = new HashSet<>();
      loopVertices.forEach(loopVertex -> {
        final IntPredicate terminationCondition = loopVertex.getTerminationCondition();
        final Integer numberOfIterations = loopVertex.getMaxNumberOfIterations();
        // We want loopVertices that are not dependent on each other or the list that is potentially going to be merged.
        final List<LoopVertex> independentLoops = loopVertices.stream().filter(loop ->
            setOfLoopsToBeFused.stream().anyMatch(list -> list.contains(loop))
                ? setOfLoopsToBeFused.stream().filter(list -> list.contains(loop))
                .findFirst()
                .map(list -> list.stream().noneMatch(loopV -> dag.pathExistsBetween(loopV, loopVertex)))
                .orElse(false)
                : !dag.pathExistsBetween(loop, loopVertex)).collect(Collectors.toList());

        // Find loops to be fused together.
        final Set<LoopVertex> loopsToBeFused = new HashSet<>();
        loopsToBeFused.add(loopVertex);
        independentLoops.forEach(independentLoop -> {
          // add them to the list if those independent loops have equal termination conditions.
          if (independentLoop.getMaxNumberOfIterations().equals(numberOfIterations)
              && checkEqualityOfIntPredicates(independentLoop.getTerminationCondition(), terminationCondition,
              numberOfIterations)) {
            loopsToBeFused.add(independentLoop);
          }
        });

        // add this information to the setOfLoopsToBeFused set.
        final Optional<Set<LoopVertex>> listToAddVerticesTo = setOfLoopsToBeFused.stream()
            .filter(list -> list.stream().anyMatch(loopsToBeFused::contains)).findFirst();
        if (listToAddVerticesTo.isPresent()) {
          listToAddVerticesTo.get().addAll(loopsToBeFused);
        } else {
          setOfLoopsToBeFused.add(loopsToBeFused);
        }
      });

      // merge and add to builder.
      setOfLoopsToBeFused.forEach(loops -> {
        if (loops.size() > 1) {
          final LoopVertex newLoopVertex = mergeLoopVertices(loops);
          builder.addVertex(newLoopVertex, dag);
          loops.forEach(loopVertex -> {
            // inEdges.
            inEdges.getOrDefault(loopVertex, new ArrayList<>()).forEach(irEdge -> {
              if (builder.contains(irEdge.getSrc())) {
                final IREdge newIREdge = new IREdge(irEdge.getPropertyValue(DataCommunicationPatternProperty.class)
                    .get(), irEdge.getSrc(), newLoopVertex, irEdge.isSideInput());
                irEdge.copyExecutionPropertiesTo(newIREdge);
                builder.connectVertices(newIREdge);
              }
            });
            // outEdges.
            outEdges.getOrDefault(loopVertex, new ArrayList<>()).forEach(irEdge -> {
              if (builder.contains(irEdge.getDst())) {
                final IREdge newIREdge = new IREdge(irEdge.getPropertyValue(DataCommunicationPatternProperty.class)
                    .get(), newLoopVertex, irEdge.getDst(), irEdge.isSideInput());
                irEdge.copyExecutionPropertiesTo(newIREdge);
                builder.connectVertices(newIREdge);
              }
            });
          });
        } else {
          loops.forEach(loopVertex -> {
            builder.addVertex(loopVertex);
            // inEdges.
            inEdges.getOrDefault(loopVertex, new ArrayList<>()).forEach(edge -> {
              if (builder.contains(edge.getSrc())) {
                builder.connectVertices(edge);
              }
            });
            // outEdges.
            outEdges.getOrDefault(loopVertex, new ArrayList<>()).forEach(edge -> {
              if (builder.contains(edge.getDst())) {
                builder.connectVertices(edge);
              }
            });
          });
        }
      });

      return builder.build();
    }

    /**
     * Merge the list of loopVertices into a single LoopVertex.
     * @param loopVertices list of LoopVertices to merge.
     * @return the merged single LoopVertex.
     */
    private LoopVertex mergeLoopVertices(final Set<LoopVertex> loopVertices) {
      final String newName =
          String.join("+", loopVertices.stream().map(LoopVertex::getName).collect(Collectors.toList()));
      final LoopVertex mergedLoopVertex = new LoopVertex(newName);
      loopVertices.forEach(loopVertex -> {
        final DAG<IRVertex, IREdge> dagToCopy = loopVertex.getDAG();
        dagToCopy.topologicalDo(v -> {
          mergedLoopVertex.getBuilder().addVertex(v);
          dagToCopy.getIncomingEdgesOf(v).forEach(mergedLoopVertex.getBuilder()::connectVertices);
        });
        loopVertex.getDagIncomingEdges().forEach((v, es) -> es.forEach(mergedLoopVertex::addDagIncomingEdge));
        loopVertex.getIterativeIncomingEdges().forEach((v, es) ->
            es.forEach(mergedLoopVertex::addIterativeIncomingEdge));
        loopVertex.getNonIterativeIncomingEdges().forEach((v, es) ->
            es.forEach(mergedLoopVertex::addNonIterativeIncomingEdge));
        loopVertex.getDagOutgoingEdges().forEach((v, es) -> es.forEach(mergedLoopVertex::addDagOutgoingEdge));
      });
      return mergedLoopVertex;
    }

    /**
     * Check the equality of two intPredicates.
     * @param predicate1 the first IntPredicate.
     * @param predicate2 the second IntPredicate.
     * @param numberToTestUntil Number to check the IntPredicates from.
     * @return whether or not we can say that they are equal.
     */
    private Boolean checkEqualityOfIntPredicates(final IntPredicate predicate1, final IntPredicate predicate2,
                                                 final Integer numberToTestUntil) {
      // TODO #11: Generalize Equality of Int Predicates for Loops.
      if (numberToTestUntil.equals(0)) {
        return predicate1.test(numberToTestUntil) == predicate2.test(numberToTestUntil);
      } else if (predicate1.test(numberToTestUntil) != predicate2.test(numberToTestUntil)) {
        return false;
      } else {
        return checkEqualityOfIntPredicates(predicate1, predicate2, numberToTestUntil - 1);
      }
    }
  }

  /**
   * Pass for Loop Invariant Code Motion optimization.
   */
  public static final class LoopInvariantCodeMotionPass extends ReshapingPass {
    /**
     * Default constructor.
     */
    public LoopInvariantCodeMotionPass() {
      super(Collections.singleton(DataCommunicationPatternProperty.class));
    }

    @Override
    public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
      final List<LoopVertex> loopVertices = new ArrayList<>();
      final Map<LoopVertex, List<IREdge>> inEdges = new HashMap<>();
      final Map<LoopVertex, List<IREdge>> outEdges = new HashMap<>();
      final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

      collectLoopVertices(dag, loopVertices, inEdges, outEdges, builder);

      // Refactor those with same data scan / operation, without dependencies in the loop.
      loopVertices.forEach(loopVertex -> {
        final List<Map.Entry<IRVertex, Set<IREdge>>> candidates = loopVertex.getNonIterativeIncomingEdges().entrySet()
            .stream().filter(entry ->
                loopVertex.getDAG().getIncomingEdgesOf(entry.getKey()).size() == 0 // no internal inEdges
                    // no external inEdges
                    && loopVertex.getIterativeIncomingEdges().getOrDefault(entry.getKey(), new HashSet<>()).size() == 0)
            .collect(Collectors.toList());
        candidates.forEach(candidate -> {
          // add refactored vertex to builder.
          builder.addVertex(candidate.getKey());
          // connect incoming edges.
          candidate.getValue().forEach(builder::connectVertices);
          // connect outgoing edges.
          loopVertex.getDAG().getOutgoingEdgesOf(candidate.getKey()).forEach(loopVertex::addDagIncomingEdge);
          loopVertex.getDAG().getOutgoingEdgesOf(candidate.getKey()).forEach(loopVertex::addNonIterativeIncomingEdge);
          // modify incoming edges of loopVertex.
          final List<IREdge> edgesToRemove = new ArrayList<>();
          final List<IREdge> edgesToAdd = new ArrayList<>();
          inEdges.getOrDefault(loopVertex, new ArrayList<>()).stream().filter(e ->
              // filter edges that have their sources as the refactored vertices.
              candidate.getValue().stream().map(IREdge::getSrc).anyMatch(edgeSrc -> edgeSrc.equals(e.getSrc())))
              .forEach(edge -> {
                edgesToRemove.add(edge);
                final IREdge newEdge = new IREdge(edge.getPropertyValue(DataCommunicationPatternProperty.class).get(),
                    candidate.getKey(), edge.getDst(), edge.isSideInput());
                newEdge.setProperty(EncoderProperty.of(edge.getPropertyValue(EncoderProperty.class).get()));
                newEdge.setProperty(DecoderProperty.of(edge.getPropertyValue(DecoderProperty.class).get()));
                edgesToAdd.add(newEdge);
              });
          final List<IREdge> listToModify = inEdges.getOrDefault(loopVertex, new ArrayList<>());
          listToModify.removeAll(edgesToRemove);
          listToModify.addAll(edgesToAdd);
          // clear garbage.
          loopVertex.getBuilder().removeVertex(candidate.getKey());
          loopVertex.getDagIncomingEdges().remove(candidate.getKey());
          loopVertex.getNonIterativeIncomingEdges().remove(candidate.getKey());
        });
      });

      // Add LoopVertices.
      loopVertices.forEach(loopVertex -> {
        builder.addVertex(loopVertex);
        inEdges.getOrDefault(loopVertex, new ArrayList<>()).forEach(builder::connectVertices);
        outEdges.getOrDefault(loopVertex, new ArrayList<>()).forEach(builder::connectVertices);
      });

      final DAG<IRVertex, IREdge> newDag = builder.build();
      if (dag.getVertices().size() == newDag.getVertices().size()) {
        return newDag;
      } else {
        return apply(newDag);
      }
    }
  }
}
