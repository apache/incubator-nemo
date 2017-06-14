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
package edu.snu.vortex.compiler.optimizer.passes;

import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.OperatorVertex;
import edu.snu.vortex.compiler.ir.SourceVertex;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.utils.dag.DAG;
import edu.snu.vortex.utils.dag.DAGBuilder;

import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;

/**
 * Optimization pass for tagging parallelism attributes.
 */
public final class ParallelismPass implements Pass {
  @Override
  public DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag) throws Exception {
    // Propagate forward source parallelism
    dag.topologicalDo(vertex -> {
      try {
        final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
        if (inEdges.isEmpty() && vertex instanceof SourceVertex) {
          final SourceVertex sourceVertex = (SourceVertex) vertex;
          vertex.setAttr(Attribute.IntegerKey.Parallelism, sourceVertex.getReaders(1).size());
        } else if (!inEdges.isEmpty()) {
          final OptionalInt parallelism = inEdges.stream()
              // No reason to propagate via Broadcast edges, as the data streams that will use the broadcasted data
              // as a sideInput will have their own number of parallelism
              .filter(edge -> !edge.getAttr(Attribute.Key.CommunicationPattern).equals(Attribute.Broadcast))
              // Let's be conservative and take the min value so that
              // the sources can support the desired parallelism in the back-propagation phase
              .mapToInt(edge -> edge.getSrc().getAttr(Attribute.IntegerKey.Parallelism)).min();
          if (parallelism.isPresent()) {
            vertex.setAttr(Attribute.IntegerKey.Parallelism, parallelism.getAsInt());
          }
          // else, this vertex only has Broadcast-type inEdges, so its number of parallelism
          // will be determined in the back-propagation phase
        } else {
          throw new RuntimeException("Weird situation: there is a non-source vertex that doesn't have any inEdges");
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Propagate backward with OneToOne edges, fixing conflicts between different sources with different parallelism
    final List<IRVertex> reverseTopologicalSort = getReverseTopologicalSort(dag);
    for (final IRVertex vertex : reverseTopologicalSort) {
      if (vertex instanceof OperatorVertex) {
        // Backward-propagate parallelism to parents with OneToOne CommunicationPattern
        final int parallelism = vertex.getAttr(Attribute.IntegerKey.Parallelism);
        dag.getIncomingEdgesOf(vertex).stream()
            .filter(edge -> edge.getAttr(Attribute.Key.CommunicationPattern) == Attribute.OneToOne)
            .map(IREdge::getSrc)
            .forEach(src -> src.setAttr(Attribute.IntegerKey.Parallelism, parallelism));
      } else if (vertex instanceof SourceVertex) {
        // Source parallelism could have been reset due to the back-propagation
        // However there can be bounds on the parallelism of sources (e.g., cannot be smaller than # of HDFS blocks)
        // We test here if the (possibly) new desired parallelism can be achieved,
        // and throw an exception if it cannot
        final SourceVertex sourceVertex = (SourceVertex) vertex;
        final Integer desiredParallelism = sourceVertex.getAttr(Attribute.IntegerKey.Parallelism);
        final Integer actualParallelism = sourceVertex.getReaders(desiredParallelism).size();
        if (!actualParallelism.equals(desiredParallelism)) {
          throw new RuntimeException("Source " + vertex.toString() + " cannot support back-propagated parallelism:"
              + "desired " + desiredParallelism + ", actual" + actualParallelism);
        }
      } else {
        throw new UnsupportedOperationException("Unknown vertex type: " + vertex.toString());
      }
    }
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>(dag);
    return builder.build();
  }

  private List<IRVertex> getReverseTopologicalSort(final DAG<IRVertex, IREdge> dag) {
    final List<IRVertex> reverseTopologicalSort = dag.getTopologicalSort();
    Collections.reverse(dag.getTopologicalSort());
    return reverseTopologicalSort;
  }
}
