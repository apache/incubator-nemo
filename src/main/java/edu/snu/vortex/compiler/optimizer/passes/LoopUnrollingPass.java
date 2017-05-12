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

import edu.snu.vortex.compiler.ir.*;
import edu.snu.vortex.utils.dag.DAG;
import edu.snu.vortex.utils.dag.DAGBuilder;

/**
 * Pass for unrolling the loops grouped by the {@link LoopGroupingPass}.
 * It first unrolls the root LoopVertex, which is in the form of linked list, into a straight line in the DAG.
 * Then, it decomposes each of the LoopVertices with the DAG information that each of them contain.
 */
public final class LoopUnrollingPass implements Pass {
  @Override
  public DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag) throws Exception {
    return recursivelyUnroll(dag);
  }

  /**
   * A function that recursively calls Unroll until there are no more LoopVertex left.
   * @param dag DAG to process.
   * @return DAG without LoopVertex.
   * @throws Exception Exceptions on the way.
   */
  private DAG<IRVertex, IREdge> recursivelyUnroll(final DAG<IRVertex, IREdge> dag) throws Exception {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();

    dag.topologicalDo(irVertex -> {
      if (irVertex instanceof LoopVertex) {
        LoopVertex loopVertex = (LoopVertex) irVertex;
        while (!loopVertex.loopTerminationConditionMet()) {
          loopVertex = loopVertex.unRollIteration(builder);
        }
      } else {
        builder.addVertex(irVertex, dag);
        dag.getIncomingEdgesOf(irVertex).forEach(builder::connectVertices);
      }
    });

    if (builder.contains((vertex) -> vertex instanceof LoopVertex)) {
      return recursivelyUnroll(builder.build());
    } else {
      return builder.build();
    }
  }
}
