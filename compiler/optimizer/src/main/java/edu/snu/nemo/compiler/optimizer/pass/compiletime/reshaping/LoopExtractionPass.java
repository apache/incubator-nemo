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
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.vertex.LoopVertex;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.common.ir.vertex.SourceVertex;

import java.util.*;

/**
 * Pass for extracting and  grouping each loops together using the LoopVertex.
 * It first groups loops together, making each iteration into a LoopOperator.
 * Then, it rolls repetitive operators into one root LoopOperator, which contains enough information to produce all
 * other iterative computations.
 */
public final class LoopExtractionPass extends ReshapingPass {
  /**
   * Default constructor.
   */
  public LoopExtractionPass() {
    super(Collections.singleton(DataCommunicationPatternProperty.class));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    final Integer maxStackDepth = this.findMaxLoopVertexStackDepth(dag);
    return groupLoops(dag, maxStackDepth);
  }

  /**
   * This method finds the maximum loop vertex stack depth of a specific DAG. This is to handle nested loops.
   * @param dag DAG to observe.
   * @return The maximum stack depth of the DAG.
   * @throws Exception exceptions through the way.
   */
  private Integer findMaxLoopVertexStackDepth(final DAG<IRVertex, IREdge> dag) {
    final OptionalInt maxDepth = dag.getVertices().stream().filter(dag::isCompositeVertex)
        .mapToInt(dag::getLoopStackDepthOf)
        .max();
    return maxDepth.orElse(0);
  }

  /**
   * This part groups each iteration of loops together by observing the LoopVertex assigned to primitive operators,
   * which is assigned by the NemoPipelineVisitor. This also shows in which depth of
   * nested loops the function handles. It recursively calls itself from the maximum depth until 0.
   * @param dag DAG to process
   * @param depth the depth of the stack to process. Must be greater than 0.
   * @return processed DAG.
   * @throws Exception exceptions through the way.
   */
  private DAG<IRVertex, IREdge> groupLoops(final DAG<IRVertex, IREdge> dag, final Integer depth) {
    if (depth <= 0) {
      return dag;
    } else {
      final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
      for (IRVertex irVertex : dag.getTopologicalSort()) {
        if (irVertex instanceof SourceVertex) { // Source vertex: no incoming edges
          if (dag.isCompositeVertex(irVertex) && dag.getLoopStackDepthOf(irVertex).equals(depth)) {
            // when src is inside a loop
            final LoopVertex assignedLoopVertex = dag.getAssignedLoopVertexOf(irVertex);
            builder.addVertex(assignedLoopVertex, dag);
            connectElementToLoop(dag, builder, irVertex, assignedLoopVertex);
          } else {
            builder.addVertex(irVertex, dag);
          }

        } else if (irVertex instanceof OperatorVertex) { // Operator vertex
          final OperatorVertex operatorVertex = (OperatorVertex) irVertex;
          // If this is Composite && depth is appropriate. == If this belongs to a loop.
          if (dag.isCompositeVertex(operatorVertex) && dag.getLoopStackDepthOf(operatorVertex).equals(depth)) {
            final LoopVertex assignedLoopVertex = dag.getAssignedLoopVertexOf(operatorVertex);
            builder.addVertex(assignedLoopVertex, dag);
            connectElementToLoop(dag, builder, operatorVertex, assignedLoopVertex); // something -> loop
          } else { // Otherwise: it is not composite || depth inappropriate. == If this is just an operator.
            builder.addVertex(operatorVertex, dag);
            dag.getIncomingEdgesOf(operatorVertex).forEach(irEdge -> {
              if (dag.isCompositeVertex(irEdge.getSrc())) {
                // connecting with a loop: loop -> operator.
                final LoopVertex srcLoopVertex = dag.getAssignedLoopVertexOf(irEdge.getSrc());
                srcLoopVertex.addDagOutgoingEdge(irEdge);
                final IREdge edgeFromLoop =
                    new IREdge(irEdge.getPropertyValue(DataCommunicationPatternProperty.class).get(),
                        srcLoopVertex, operatorVertex, irEdge.isSideInput());
                irEdge.copyExecutionPropertiesTo(edgeFromLoop);
                builder.connectVertices(edgeFromLoop);
                srcLoopVertex.mapEdgeWithLoop(edgeFromLoop, irEdge);
              } else { // connecting outside the composite loop: operator -> operator.
                builder.connectVertices(irEdge);
              }
            });
          }

        } else if (irVertex instanceof LoopVertex) { // Loop vertices of higher depth (nested loops).
          final LoopVertex loopVertex = (LoopVertex) irVertex;
          if (dag.isCompositeVertex(loopVertex)) { // the loopVertex belongs to another loop.
            final LoopVertex assignedLoopVertex = dag.getAssignedLoopVertexOf(loopVertex);
            connectElementToLoop(dag, builder, loopVertex, assignedLoopVertex); // something -> loop
          } else { // it cannot be just at the operator level, as it had more depth.
            throw new UnsupportedOperationException("This loop (" + loopVertex + ") shouldn't be of this depth");
          }

        } else {
          throw new UnsupportedOperationException("Unknown vertex type: " + irVertex);
        }
      }

      // Recursive calls for lower depths.
      return groupLoops(loopRolling(builder.build()), depth - 1);
    }
  }

  /**
   * Method for connecting an element to a loop. That is, loop -> loop OR operator -> loop.
   * @param dag to observe the inEdges from.
   * @param builder to add the new edge to.
   * @param dstVertex the destination vertex that belongs to a certain loop.
   * @param assignedLoopVertex the loop that dstVertex belongs to.
   */
  private static void connectElementToLoop(final DAG<IRVertex, IREdge> dag, final DAGBuilder<IRVertex, IREdge> builder,
                                           final IRVertex dstVertex, final LoopVertex assignedLoopVertex) {
    assignedLoopVertex.getBuilder().addVertex(dstVertex, dag);

    dag.getIncomingEdgesOf(dstVertex).forEach(irEdge -> {
      if (dag.isCompositeVertex(irEdge.getSrc())) {
        final LoopVertex srcLoopVertex = dag.getAssignedLoopVertexOf(irEdge.getSrc());
        if (srcLoopVertex.equals(assignedLoopVertex)) { // connecting within the composite loop DAG.
          assignedLoopVertex.getBuilder().connectVertices(irEdge);
        } else { // loop -> loop connection
          assignedLoopVertex.addDagIncomingEdge(irEdge);
          final IREdge edgeToLoop = new IREdge(irEdge.getPropertyValue(DataCommunicationPatternProperty.class).get(),
              srcLoopVertex, assignedLoopVertex, irEdge.isSideInput());
          irEdge.copyExecutionPropertiesTo(edgeToLoop);
          builder.connectVertices(edgeToLoop);
          assignedLoopVertex.mapEdgeWithLoop(edgeToLoop, irEdge);
        }
      } else { // operator -> loop
        assignedLoopVertex.addDagIncomingEdge(irEdge);
        final IREdge edgeToLoop = new IREdge(irEdge.getPropertyValue(DataCommunicationPatternProperty.class).get(),
            irEdge.getSrc(), assignedLoopVertex, irEdge.isSideInput());
        irEdge.copyExecutionPropertiesTo(edgeToLoop);
        builder.connectVertices(edgeToLoop);
        assignedLoopVertex.mapEdgeWithLoop(edgeToLoop, irEdge);
      }
    });
  }

  /**
   * This part rolls the repetitive LoopVertices into a single one, leaving only the root LoopVertex.
   * Following iterations can be generated with the information included in the LoopVertex.
   * @param dag DAG to process.
   * @return Processed DAG.
   * @throws Exception exceptions through the way.
   */
  private DAG<IRVertex, IREdge> loopRolling(final DAG<IRVertex, IREdge> dag) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    final HashMap<LoopVertex, LoopVertex> loopVerticesOfSameLoop = new HashMap<>();
    final HashMap<LoopVertex, HashMap<IRVertex, IRVertex>> equivalentVerticesOfLoops = new HashMap<>();
    LoopVertex rootLoopVertex = null;

    // observe the DAG in a topological order.
    for (IRVertex irVertex : dag.getTopologicalSort()) {
      if (irVertex instanceof SourceVertex) { // source vertex
        builder.addVertex(irVertex, dag);

      } else if (irVertex instanceof OperatorVertex) { // operator vertex
        addVertexToBuilder(builder, dag, irVertex, loopVerticesOfSameLoop);

      } else if (irVertex instanceof LoopVertex) { // loop vertex: we roll them if it is not root
        final LoopVertex loopVertex = (LoopVertex) irVertex;

        if (rootLoopVertex == null || !loopVertex.getName().contains(rootLoopVertex.getName())) { // initial root loop
          rootLoopVertex = loopVertex;
          loopVerticesOfSameLoop.putIfAbsent(rootLoopVertex, rootLoopVertex);
          equivalentVerticesOfLoops.putIfAbsent(rootLoopVertex, new HashMap<>());
          for (IRVertex vertex : rootLoopVertex.getDAG().getTopologicalSort()) {
            equivalentVerticesOfLoops.get(rootLoopVertex).putIfAbsent(vertex, vertex);
          }
          addVertexToBuilder(builder, dag, rootLoopVertex, loopVerticesOfSameLoop);
        } else { // following loops
          final LoopVertex finalRootLoopVertex = rootLoopVertex;

          // Add the loop to the list
          loopVerticesOfSameLoop.putIfAbsent(loopVertex, finalRootLoopVertex);
          finalRootLoopVertex.increaseMaxNumberOfIterations();

          // Zip current vertices together. We rely on the fact that getTopologicalSort() brings consistent results.
          final Iterator<IRVertex> verticesOfRootLoopVertex =
              finalRootLoopVertex.getDAG().getTopologicalSort().iterator();
          final Iterator<IRVertex> verticesOfCurrentLoopVertex = loopVertex.getDAG().getTopologicalSort().iterator();
          final HashMap<IRVertex, IRVertex> equivalentVertices = equivalentVerticesOfLoops.get(finalRootLoopVertex);
          while (verticesOfRootLoopVertex.hasNext() && verticesOfCurrentLoopVertex.hasNext()) {
            equivalentVertices.put(verticesOfCurrentLoopVertex.next(), verticesOfRootLoopVertex.next());
          }

          // reset non iterative incoming edges.
          finalRootLoopVertex.getNonIterativeIncomingEdges().clear();
          finalRootLoopVertex.getIterativeIncomingEdges().clear();

          // incoming edges to the DAG.
          loopVertex.getDagIncomingEdges().forEach((dstVertex, edges) -> edges.forEach(edge -> {
            final IRVertex srcVertex = edge.getSrc();
            final IRVertex equivalentDstVertex = equivalentVertices.get(dstVertex);

            if (equivalentVertices.containsKey(srcVertex)) {
              // src is from the previous loop. vertex in previous loop -> DAG.
              final IRVertex equivalentSrcVertex = equivalentVertices.get(srcVertex);

              // add the new IREdge to the iterative incoming edges list.
              final IREdge newIrEdge = new IREdge(edge.getPropertyValue(DataCommunicationPatternProperty.class).get(),
                  equivalentSrcVertex, equivalentDstVertex, edge.isSideInput());
              edge.copyExecutionPropertiesTo(newIrEdge);
              finalRootLoopVertex.addIterativeIncomingEdge(newIrEdge);
            } else {
              // src is from outside the previous loop. vertex outside previous loop -> DAG.
              final IREdge newIrEdge = new IREdge(edge.getPropertyValue(DataCommunicationPatternProperty.class).get(),
                  srcVertex, equivalentDstVertex, edge.isSideInput());
              edge.copyExecutionPropertiesTo(newIrEdge);
              finalRootLoopVertex.addNonIterativeIncomingEdge(newIrEdge);
            }
          }));

          // Overwrite the DAG outgoing edges
          finalRootLoopVertex.getDagOutgoingEdges().clear();
          loopVertex.getDagOutgoingEdges().forEach((srcVertex, edges) -> edges.forEach(edge -> {
            final IRVertex dstVertex = edge.getDst();
            final IRVertex equivalentSrcVertex = equivalentVertices.get(srcVertex);

            final IREdge newIrEdge = new IREdge(edge.getPropertyValue(DataCommunicationPatternProperty.class).get(),
                equivalentSrcVertex, dstVertex, edge.isSideInput());
            edge.copyExecutionPropertiesTo(newIrEdge);
            finalRootLoopVertex.addDagOutgoingEdge(newIrEdge);
            finalRootLoopVertex.mapEdgeWithLoop(loopVertex.getEdgeWithLoop(edge), newIrEdge);
          }));
        }

      } else {
        throw new UnsupportedOperationException("Unknown vertex type: " + irVertex);
      }
    }

    return builder.build();
  }

  /**
   * Adds the vertex and the incoming edges of the vertex to the builder.
   * @param builder Builder that it adds to.
   * @param irVertex Vertex to add.
   * @param dag DAG to observe the incoming edges of the vertex.
   * @param loopVerticesOfSameLoop List that keeps track of the iterations of the identical loop.
   */
  private static void addVertexToBuilder(final DAGBuilder<IRVertex, IREdge> builder, final DAG<IRVertex, IREdge> dag,
                                         final IRVertex irVertex,
                                         final Map<LoopVertex, LoopVertex> loopVerticesOfSameLoop) {
    builder.addVertex(irVertex, dag);
    dag.getIncomingEdgesOf(irVertex).forEach(edge -> {

      // find first LoopVertex of the loop, if it exists. Otherwise just use the src.
      final IRVertex firstEquivalentVertex;
      if (edge.getSrc() instanceof LoopVertex) {
        final LoopVertex equivalentVertexCandidate = loopVerticesOfSameLoop.get(edge.getSrc());
        if (equivalentVertexCandidate != null) {
          firstEquivalentVertex = equivalentVertexCandidate;
        } else {
          firstEquivalentVertex = edge.getSrc();
        }
      } else {
        firstEquivalentVertex = edge.getSrc();
      }

      if (edge.getSrc().equals(firstEquivalentVertex)) {
        builder.connectVertices(edge);
      } else {
        final IREdge newIrEdge = new IREdge(edge.getPropertyValue(DataCommunicationPatternProperty.class).get(),
            firstEquivalentVertex, irVertex, edge.isSideInput());
        edge.copyExecutionPropertiesTo(newIrEdge);
        builder.connectVertices(newIrEdge);
      }
    });
  }
}
