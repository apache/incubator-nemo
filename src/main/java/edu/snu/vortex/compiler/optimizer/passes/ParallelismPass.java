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
import edu.snu.vortex.compiler.ir.SourceVertex;
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.utils.dag.DAG;

import java.util.Set;

/**
 * Optimization pass for tagging parallelism attributes.
 */
public final class ParallelismPass implements Pass {
  public DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag) throws Exception {
    dag.topologicalDo(vertex -> {
      try {
        final Set<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
        if (inEdges.isEmpty() && vertex instanceof SourceVertex) {
          final SourceVertex sourceVertex = (SourceVertex) vertex;
          vertex.setAttr(Attribute.IntegerKey.Parallelism, sourceVertex.getReaders(1).size());
        } else if (!inEdges.isEmpty()) {
          Integer parallelism = inEdges.stream()
              .mapToInt(edge -> edge.getSrc().getAttr(Attribute.IntegerKey.Parallelism)).max().getAsInt();
          vertex.setAttr(Attribute.IntegerKey.Parallelism, parallelism);
        } else {
          throw new RuntimeException("Weird situation: there is a non-source vertex that doesn't have any inEdges");
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    return dag;
  }
}
