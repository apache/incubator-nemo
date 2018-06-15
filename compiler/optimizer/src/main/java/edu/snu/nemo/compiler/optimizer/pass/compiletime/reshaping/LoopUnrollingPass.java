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

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.dag.DAGBuilder;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.LoopVertex;

/**
 * Pass for unrolling the loops grouped by the {@link LoopExtractionPass}.
 * It first unrolls the root LoopVertex, which is in the form of linked list, into a straight line in the DAG.
 * Then, it decomposes each of the LoopVertices with the DAG information that each of them contain.
 */
public final class LoopUnrollingPass extends ReshapingPass {
  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    return recursivelyUnroll(dag);
  }

  /**
   * A function that recursively calls Unroll until there are no more LoopVertex left.
   * @param dag DAG to process.
   * @return DAG without LoopVertex.
   */
  private DAG<IRVertex, IREdge> recursivelyUnroll(final DAG<IRVertex, IREdge> dag) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    dag.topologicalDo(irVertex -> {
      if (irVertex instanceof LoopVertex) {
        LoopVertex loopVertex = (LoopVertex) irVertex;
        loopVertex.markDuplicateEdges();
        while (!loopVertex.loopTerminationConditionMet()) {
          loopVertex = loopVertex.unRollIteration(builder);
        }
      } else {
        builder.addVertex(irVertex, dag);
        dag.getIncomingEdgesOf(irVertex).forEach(e -> {
          if (!(e.getSrc() instanceof LoopVertex)) {
            builder.connectVertices(e);
          }
        });
      }
    });

    if (builder.contains((vertex) -> vertex instanceof LoopVertex)) {
      return recursivelyUnroll(builder.build());
    } else {
      return builder.build();
    }
  }
}
