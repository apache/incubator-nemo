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
package edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.onyx.common.dag.DAGBuilder;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.ir.vertex.SourceVertex;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.common.ir.vertex.executionproperty.ParallelismProperty;

import java.util.List;

/**
 * Optimization pass for tagging parallelism execution property.
 */
public final class DefaultParallelismPass extends AnnotatingPass {
  // we decrease the number of parallelism by this number on each shuffle boundary.
  private final Integer shuffleDecreaseFactor = 2;

  /**
   * Default constructor.
   */
  public DefaultParallelismPass() {
    super(ExecutionProperty.Key.Parallelism);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    // Propagate forward source parallelism
    dag.topologicalDo(vertex -> {
      try {
        final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
        // We manipulate them if it is set as default value of 1.
        if (inEdges.isEmpty() && vertex instanceof SourceVertex) {
          final SourceVertex sourceVertex = (SourceVertex) vertex;
          vertex.setProperty(ParallelismProperty.of(sourceVertex.getReaders(1).size()));
        } else if (!inEdges.isEmpty()) {
          // No reason to propagate via Broadcast edges, as the data streams that will use the broadcasted data
          // as a sideInput will have their own number of parallelism
          final Integer o2oParallelism = inEdges.stream()
             .filter(edge -> DataCommunicationPatternProperty.Value.OneToOne
                  .equals(edge.getProperty(ExecutionProperty.Key.DataCommunicationPattern)))
              .mapToInt(edge -> edge.getSrc().getProperty(ExecutionProperty.Key.Parallelism))
              .max().orElse(1);
          final Integer shuffleParallelism = inEdges.stream()
              .filter(edge -> DataCommunicationPatternProperty.Value.Shuffle
                  .equals(edge.getProperty(ExecutionProperty.Key.DataCommunicationPattern)))
              .mapToInt(edge -> edge.getSrc().getProperty(ExecutionProperty.Key.Parallelism))
              .map(i -> i / shuffleDecreaseFactor)
              .max().orElse(1);
          // We set the greater value as the parallelism.
          final Integer parallelism = o2oParallelism > shuffleParallelism ? o2oParallelism : shuffleParallelism;
          vertex.setProperty(ParallelismProperty.of(parallelism));
          // synchronize one-to-one edges parallelism
          recursivelySynchronizeO2OParallelism(dag, vertex, parallelism);
        } else if (vertex.getProperty(ExecutionProperty.Key.Parallelism) == null) {
          throw new RuntimeException("There is a non-source vertex that doesn't have any inEdges "
              + "(excluding SideInput edges)");
        } // No problem otherwise.
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);
    return builder.build();
  }

  /**
   * Recursively synchronize parallelism for vertices connected by one-to-one edges.
   * @param dag the original DAG.
   * @param vertex vertex to observe and update.
   * @param parallelism the parallelism of the most recently updated descendant.
   * @return the max value of parallelism among those observed.
   */
  static Integer recursivelySynchronizeO2OParallelism(final DAG<IRVertex, IREdge> dag, final IRVertex vertex,
                                                      final Integer parallelism) {
    final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
    final Integer ancestorParallelism = inEdges.stream()
        .filter(edge -> DataCommunicationPatternProperty.Value.OneToOne
            .equals(edge.getProperty(ExecutionProperty.Key.DataCommunicationPattern)))
        .map(IREdge::getSrc)
        .mapToInt(inVertex -> recursivelySynchronizeO2OParallelism(dag, inVertex, parallelism))
        .max().orElse(1);
    final Integer maxParallelism = ancestorParallelism > parallelism ? ancestorParallelism : parallelism;
    final Integer myParallelism = vertex.getProperty(ExecutionProperty.Key.Parallelism);

    // update the vertex with the max value.
    if (maxParallelism > myParallelism) {
      vertex.setProperty(ParallelismProperty.of(maxParallelism));
      return maxParallelism;
    }
    return myParallelism;
  }
}
