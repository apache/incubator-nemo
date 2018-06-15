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
package edu.snu.nemo.compiler.optimizer;

import edu.snu.nemo.common.exception.CompileTimeOptimizationException;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.reshaping.ReshapingPass;
import edu.snu.nemo.compiler.optimizer.policy.Policy;

import java.util.*;

/**
 * Compile time optimizer class.
 */
public final class CompiletimeOptimizer {
  /**
   * Private constructor.
   */
  private CompiletimeOptimizer() {
  }

  /**
   * Optimize function.
   * @param dag input DAG.
   * @param optimizationPolicy the optimization policy that we want to use to optimize the DAG.
   * @param dagDirectory directory to save the DAG information.
   * @return optimized DAG, tagged with execution properties.
   * @throws Exception throws an exception if there is an exception.
   */
  public static DAG<IRVertex, IREdge> optimize(final DAG<IRVertex, IREdge> dag, final Policy optimizationPolicy,
                                               final String dagDirectory) throws Exception {
    if (optimizationPolicy == null || optimizationPolicy.getCompileTimePasses().isEmpty()) {
      throw new CompileTimeOptimizationException("A policy name should be specified.");
    }
    return process(dag, optimizationPolicy.getCompileTimePasses().iterator(), dagDirectory);
  }

  /**
   * A recursive method to process each pass one-by-one to the given DAG.
   * @param dag DAG to process.
   * @param passes passes to apply.
   * @param dagDirectory directory to save the DAG information.
   * @return the processed DAG.
   * @throws Exception Exceptions on the way.
   */
  private static DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag,
                                               final Iterator<CompileTimePass> passes,
                                               final String dagDirectory) throws Exception {
    if (passes.hasNext()) {
      final CompileTimePass passToApply = passes.next();
      // Apply the pass to the DAG.
      final DAG<IRVertex, IREdge> processedDAG = passToApply.apply(dag);
      // Ensure AnnotatingPass and ReshapingPass functions as intended.
      if ((passToApply instanceof AnnotatingPass && !checkAnnotatingPass(dag, processedDAG))
          || (passToApply instanceof ReshapingPass && !checkReshapingPass(dag, processedDAG))) {
        throw new CompileTimeOptimizationException(passToApply.getClass().getSimpleName()
            + " is implemented in a way that doesn't follow its original intention of annotating or reshaping. "
            + "Modify it or use a general CompileTimePass");
      }
      // Save the processed JSON DAG.
      processedDAG.storeJSON(dagDirectory, "ir-after-" + passToApply.getClass().getSimpleName(),
          "DAG after optimization");
      // recursively apply the following passes.
      return process(processedDAG, passes, dagDirectory);
    } else {
      return dag;
    }
  }

  /**
   * Checks if the annotating pass hasn't modified the DAG structure.
   * It checks if the number of Vertices and Edges are the same.
   * @param before DAG before modification.
   * @param after DAG after modification.
   * @return true if there is no problem, false if there is a problem.
   */
  private static Boolean checkAnnotatingPass(final DAG<IRVertex, IREdge> before, final DAG<IRVertex, IREdge> after) {
    final Iterator<IRVertex> beforeVertices = before.getTopologicalSort().iterator();
    final Iterator<IRVertex> afterVertices = after.getTopologicalSort().iterator();
    while (beforeVertices.hasNext() && afterVertices.hasNext()) {
      final IRVertex beforeVertex = beforeVertices.next();
      final IRVertex afterVertex = afterVertices.next();
      // each of vertices should have same ids.
      if (!beforeVertex.getId().equals(afterVertex.getId())) {
        return false;
      }
      final Iterator<IREdge> beforeVertexIncomingEdges = before.getIncomingEdgesOf(beforeVertex).iterator();
      final Iterator<IREdge> afterVertexIncomingEdges = after.getIncomingEdgesOf(afterVertex).iterator();
      final Iterator<IREdge> beforeVertexOutgoingEdges = before.getOutgoingEdgesOf(beforeVertex).iterator();
      final Iterator<IREdge> afterVertexOutgoingEdges = after.getOutgoingEdgesOf(afterVertex).iterator();
      while (beforeVertexIncomingEdges.hasNext() && afterVertexIncomingEdges.hasNext()) {
        // each of them should have same ids.
        if (!beforeVertexIncomingEdges.next().getId().equals(afterVertexIncomingEdges.next().getId())) {
          return false;
        }
      }
      while (beforeVertexOutgoingEdges.hasNext() && afterVertexOutgoingEdges.hasNext()) {
        // each of them should have same ids.
        if (!beforeVertexOutgoingEdges.next().getId().equals(afterVertexOutgoingEdges.next().getId())) {
          return false;
        }
      }
      // number of edges should match.
      if (beforeVertexIncomingEdges.hasNext() || afterVertexIncomingEdges.hasNext()
          || beforeVertexOutgoingEdges.hasNext() || afterVertexOutgoingEdges.hasNext()) {
        return false;
      }
    }
    // number of vertices should match.
    return !beforeVertices.hasNext() && !afterVertices.hasNext();
  }

  /**
   * Checks if the reshaping pass hasn't modified execution properties.
   * It checks if all of its vertices and edges have the same execution properties as before (if it existed then).
   * @param before DAG before modification.
   * @param after DAG after modification.
   * @return true if there is no problem, false if there is a problem.
   */
  private static Boolean checkReshapingPass(final DAG<IRVertex, IREdge> before, final DAG<IRVertex, IREdge> after) {
    final List<IRVertex> previousVertices = before.getVertices();
    for (final IRVertex irVertex : after.getVertices()) {
      final Integer indexOfVertex = previousVertices.indexOf(irVertex);
      if (indexOfVertex >= 0) {
        final IRVertex previousVertexToCompare = previousVertices.get(indexOfVertex);
        if (!previousVertexToCompare.getExecutionProperties().equals(irVertex.getExecutionProperties())) {
          return false;
        }
        for (final IREdge irEdge : after.getIncomingEdgesOf(irVertex)) {
          final Integer indexOfEdge = before.getIncomingEdgesOf(previousVertexToCompare).indexOf(irEdge);
          if (indexOfEdge >= 0) {
            final IREdge previousIREdgeToCompare = before.getIncomingEdgesOf(previousVertexToCompare).get(indexOfEdge);
            if (!previousIREdgeToCompare.getExecutionProperties().equals(irEdge.getExecutionProperties())) {
              return false;
            }
          }
        }
      }
    }
    return true;
  }
}
