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

import edu.snu.nemo.common.coder.DecoderFactory;
import edu.snu.nemo.common.coder.EncoderFactory;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DecoderProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.EncoderProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Pass for Common Subexpression Elimination optimization. It eliminates vertices that are repetitively run without
 * much meaning, and runs it a single time, instead of multiple times. We consider such vertices as 'common' when
 * they include the same transform, and has incoming edges from an identical set of vertices.
 * Refer to CommonSubexpressionEliminationPassTest for such cases.
 */
public final class CommonSubexpressionEliminationPass extends ReshapingPass {
  /**
   * Default constructor.
   */
  public CommonSubexpressionEliminationPass() {
    super(Collections.singleton(DataCommunicationPatternProperty.class));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    // find and collect vertices with equivalent transforms
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    final Map<Transform, List<OperatorVertex>> operatorVerticesToBeMerged = new HashMap<>();
    final Map<OperatorVertex, Set<IREdge>> inEdges = new HashMap<>();
    final Map<OperatorVertex, Set<IREdge>> outEdges = new HashMap<>();

    dag.topologicalDo(irVertex -> {
      if (irVertex instanceof OperatorVertex) {
        final OperatorVertex operatorVertex = (OperatorVertex) irVertex;
        operatorVerticesToBeMerged.putIfAbsent(operatorVertex.getTransform(), new ArrayList<>());
        operatorVerticesToBeMerged.get(operatorVertex.getTransform()).add(operatorVertex);

        dag.getIncomingEdgesOf(operatorVertex).forEach(irEdge -> {
          inEdges.putIfAbsent(operatorVertex, new HashSet<>());
          inEdges.get(operatorVertex).add(irEdge);
          if (irEdge.getSrc() instanceof OperatorVertex) {
            final OperatorVertex source = (OperatorVertex) irEdge.getSrc();
            outEdges.putIfAbsent(source, new HashSet<>());
            outEdges.get(source).add(irEdge);
          }
        });
      } else {
        builder.addVertex(irVertex, dag);
        dag.getIncomingEdgesOf(irVertex).forEach(irEdge -> {
          if (irEdge.getSrc() instanceof OperatorVertex) {
            final OperatorVertex source = (OperatorVertex) irEdge.getSrc();
            outEdges.putIfAbsent(source, new HashSet<>());
            outEdges.get(source).add(irEdge);
          } else {
            builder.connectVertices(irEdge);
          }
        });
      }
    });

    // merge them if they are not dependent on each other, and add IRVertices to the builder.
    operatorVerticesToBeMerged.forEach(((transform, operatorVertices) -> {
      final Map<Set<IRVertex>, List<OperatorVertex>> verticesToBeMergedWithIdenticalSources = new HashMap<>();

      operatorVertices.forEach(operatorVertex -> {
        // compare if incoming vertices are identical.
        final Set<IRVertex> incomingVertices = dag.getIncomingEdgesOf(operatorVertex).stream().map(IREdge::getSrc)
            .collect(Collectors.toSet());
        if (verticesToBeMergedWithIdenticalSources.keySet().stream()
            .anyMatch(lst -> lst.containsAll(incomingVertices) && incomingVertices.containsAll(lst))) {
          final Set<IRVertex> foundKey = verticesToBeMergedWithIdenticalSources.keySet().stream()
              .filter(vs -> vs.containsAll(incomingVertices) && incomingVertices.containsAll(vs))
              .findFirst().get();
          verticesToBeMergedWithIdenticalSources.get(foundKey).add(operatorVertex);
        } else {
          verticesToBeMergedWithIdenticalSources.putIfAbsent(incomingVertices, new ArrayList<>());
          verticesToBeMergedWithIdenticalSources.get(incomingVertices).add(operatorVertex);
        }
      });

      verticesToBeMergedWithIdenticalSources.values().forEach(ovs ->
          mergeAndAddToBuilder(ovs, builder, dag, inEdges, outEdges));
    }));

    // process IREdges
    operatorVerticesToBeMerged.values().forEach(operatorVertices ->
        operatorVertices.forEach(operatorVertex -> {
          inEdges.getOrDefault(operatorVertex, new HashSet<>()).forEach(e -> {
            if (builder.contains(operatorVertex) && builder.contains(e.getSrc())) {
              builder.connectVertices(e);
            }
          });
          outEdges.getOrDefault(operatorVertex, new HashSet<>()).forEach(e -> {
            if (builder.contains(operatorVertex) && builder.contains(e.getDst())) {
              builder.connectVertices(e);
            }
          });
        }));

    return builder.build();
  }

  /**
   * merge equivalent operator vertices and add them to the provided builder.
   * @param ovs operator vertices that are to be merged (if there are no dependencies between them).
   * @param builder builder to add the merged vertices to.
   * @param dag dag to observe while adding them.
   * @param inEdges incoming edges information.
   * @param outEdges outgoing edges information.
   */
  private static void mergeAndAddToBuilder(final List<OperatorVertex> ovs, final DAGBuilder<IRVertex, IREdge> builder,
                                           final DAG<IRVertex, IREdge> dag,
                                           final Map<OperatorVertex, Set<IREdge>> inEdges,
                                           final Map<OperatorVertex, Set<IREdge>> outEdges) {
    if (ovs.size() > 0) {
      final OperatorVertex operatorVertexToUse = ovs.get(0);
      final List<OperatorVertex> dependencyFailedOperatorVertices = new ArrayList<>();

      builder.addVertex(operatorVertexToUse);
      ovs.forEach(ov -> {
        if (!ov.equals(operatorVertexToUse)) {
          if (dag.pathExistsBetween(operatorVertexToUse, ov)) {
            dependencyFailedOperatorVertices.add(ov);
          } else {
            // incoming edges do not need to be considered, as they come from identical incoming vertices.
            // process outEdges
            final Set<IREdge> outListToModify = outEdges.get(ov);
            outEdges.getOrDefault(ov, new HashSet<>()).forEach(e -> {
              outListToModify.remove(e);
              final IREdge newIrEdge = new IREdge(e.getPropertyValue(DataCommunicationPatternProperty.class).get(),
                  operatorVertexToUse, e.getDst());
              final Optional<EncoderFactory> encoderProperty = e.getPropertyValue(EncoderProperty.class);
              if (encoderProperty.isPresent()) {
                newIrEdge.setProperty(EncoderProperty.of(encoderProperty.get()));
              }
              final Optional<DecoderFactory> decoderProperty = e.getPropertyValue(DecoderProperty.class);
              if (decoderProperty.isPresent()) {
                newIrEdge.setProperty(DecoderProperty.of(decoderProperty.get()));
              }
              outListToModify.add(newIrEdge);
            });
            outEdges.remove(ov);
            outEdges.putIfAbsent(operatorVertexToUse, new HashSet<>());
            outEdges.get(operatorVertexToUse).addAll(outListToModify);
          }
        }
      });
      mergeAndAddToBuilder(dependencyFailedOperatorVertices, builder, dag, inEdges, outEdges);
    }
  }
}
