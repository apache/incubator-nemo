/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.compiler.optimizer.policy;

import org.apache.nemo.common.exception.CompileTimeOptimizationException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.IRDAGChecker;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.CompileTimePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.AnnotatingPass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping.ReshapingPass;
import org.apache.nemo.compiler.optimizer.pass.runtime.Message;
import org.apache.nemo.compiler.optimizer.pass.runtime.RunTimePass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Implementation of the {@link Policy} interface.
 */
public final class PolicyImpl implements Policy {
  private final List<CompileTimePass> compileTimePasses;
  private final Set<RunTimePass<?>> runTimePasses;
  private static final Logger LOG = LoggerFactory.getLogger(PolicyImpl.class.getName());

  /**
   * Constructor.
   *
   * @param compileTimePasses compile time passes of the policy.
   * @param runTimePasses     run time passes of the policy.
   */
  public PolicyImpl(final List<CompileTimePass> compileTimePasses, final Set<RunTimePass<?>> runTimePasses) {
    this.compileTimePasses = compileTimePasses;
    this.runTimePasses = runTimePasses;
  }

  @Override
  public IRDAG runCompileTimeOptimization(final IRDAG dag, final String dagDirectory) {
    LOG.info("Launch Compile-time optimizations");
    return process(dag, compileTimePasses.iterator(), dagDirectory);
  }

  /**
   * A recursive method to process each pass one-by-one to the given DAG.
   *
   * @param dag          DAG to process.
   * @param passes       passes to apply.
   * @param dagDirectory directory to save the DAG information.
   * @return the processed DAG.
   * @throws Exception Exceptions on the way.
   */
  private static IRDAG process(final IRDAG dag,
                               final Iterator<CompileTimePass> passes,
                               final String dagDirectory) {
    if (passes.hasNext()) {
      final CompileTimePass passToApply = passes.next();
      final IRDAG processedDAG;

      if (passToApply.getCondition().test(dag)) {
        LOG.info("Apply {} to the DAG", passToApply.getClass().getSimpleName());
        // Apply the pass to the DAG.
        processedDAG = passToApply.apply(dag);

        final boolean advanced = processedDAG.advanceDAGSnapshot((beforePass, afterPass) ->
          // Ensure AnnotatingPass and ReshapingPass functions as intended.
          !((passToApply instanceof AnnotatingPass && !checkAnnotatingPass(beforePass, afterPass))
            || (passToApply instanceof ReshapingPass && !checkReshapingPass(beforePass, afterPass))));

        if (!advanced) {
          throw new CompileTimeOptimizationException(passToApply.getClass().getSimpleName()
            + " is implemented in a way that doesn't follow its original intention of annotating or reshaping. "
            + "Modify it or use a general CompileTimePass");
        }

        final IRDAGChecker.CheckerResult integrity = processedDAG.checkIntegrity();
        if (!integrity.isPassed()) {
          final long curTime = System.currentTimeMillis();
          processedDAG.storeJSON(dagDirectory, String.valueOf(curTime), "integrity failure");
          throw new CompileTimeOptimizationException(integrity.getFailReason()
            + " / For DAG visualization, check out " + dagDirectory + curTime + ".json");
        }

        // Save the processed JSON DAG.
        processedDAG.storeJSON(dagDirectory, "ir-after-" + passToApply.getClass().getSimpleName(),
          "DAG after optimization");
      } else {
        LOG.info("Condition unmet for applying {} to the DAG", passToApply.getClass().getSimpleName());
        processedDAG = dag;
      }
      // recursively apply the following passes.
      return process(processedDAG, passes, dagDirectory);
    } else {
      return dag;
    }
  }

  /**
   * Checks if the annotating pass hasn't modified the DAG structure.
   * It checks if the number of Vertices and Edges are the same.
   *
   * @param before DAG before modification.
   * @param after  DAG after modification.
   * @return true if there is no problem, false if there is a problem.
   */
  private static Boolean checkAnnotatingPass(final IRDAG before, final IRDAG after) {
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
   *
   * @param before DAG before modification.
   * @param after  DAG after modification.
   * @return true if there is no problem, false if there is a problem.
   */
  private static Boolean checkReshapingPass(final IRDAG before, final IRDAG after) {
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

  @Override
  public IRDAG runRunTimeOptimizations(final IRDAG irdag, final Message message) {
    runTimePasses.forEach(p -> {
      final IRDAG processedDAG = p.apply(irdag, message);
      final IRDAGChecker.CheckerResult integrity = processedDAG.checkIntegrity();
      if (!integrity.isPassed()) {
        throw new CompileTimeOptimizationException(integrity.getFailReason());
      }
    });
    return irdag;
  }
}
