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
package edu.snu.vortex.compiler.optimizer;

import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.optimizer.passes.*;
import edu.snu.vortex.utils.dag.DAG;

import java.util.*;

/**
 * Optimizer class.
 */
public final class Optimizer {
  /**
   * Optimize function.
   * @param dag input DAG.
   * @param policyType type of the instantiation policy that we want to use to optimize the DAG.
   * @return optimized DAG, tagged with attributes.
   * @throws Exception throws an exception if there is an exception.
   */
  public DAG<IRVertex, IREdge> optimize(final DAG<IRVertex, IREdge> dag, final PolicyType policyType) throws Exception {
    if (policyType == null) {
      throw new RuntimeException("Policy has not been provided for the policyType");
    }
    return process(dag, POLICIES.get(policyType));
  }

  /**
   * A recursive method to process each pass one-by-one to the given DAG.
   * @param dag DAG to process.
   * @param passes passes to apply.
   * @return the processed DAG.
   * @throws Exception Exceptionso n the way.
   */
  private static DAG<IRVertex, IREdge> process(final DAG<IRVertex, IREdge> dag, final List<Pass> passes)
          throws Exception {
    if (passes.isEmpty()) {
      return dag;
    } else {
      return process(passes.get(0).process(dag), passes.subList(1, passes.size()));
    }
  }

  /**
   * Enum for different types of instantiation policies.
   */
  public enum PolicyType {
    None,
    Pado,
    Disaggregation,
  }

  /**
   * A HashMap to match each of instantiation policies with a combination of instantiation passes.
   * Each policies are run in the order with which they are defined.
   */
  private static final Map<PolicyType, List<Pass>> POLICIES = new HashMap<>();
  static {
    POLICIES.put(PolicyType.None,
        Arrays.asList(
            new ParallelismPass() // Provides parallelism information.
        ));
    POLICIES.put(PolicyType.Pado,
        Arrays.asList(
            new ParallelismPass(), // Provides parallelism information.
            new LoopGroupingPass(),
            new LoopUnrollingPass(), // Groups then unrolls loops. TODO #162: remove unrolling pt.
            new PadoVertexPass(), new PadoEdgePass() // Processes vertices and edges with Pado algorithm.
        ));
    POLICIES.put(PolicyType.Disaggregation,
        Arrays.asList(
            new ParallelismPass(), // Provides parallelism information.
            new LoopGroupingPass(),
            new LoopUnrollingPass(), // Groups then unrolls loops. TODO #162: remove unrolling pt.
            new DisaggregationPass() // Processes vertices and edges with Disaggregation algorithm.
        ));
  }

  /**
   * A HashMap to convert string names for each policy type to receive as arguments.
   */
  public static final Map<String, PolicyType> POLICY_NAME = new HashMap<>();
  static {
    POLICY_NAME.put("none", PolicyType.None);
    POLICY_NAME.put("pado", PolicyType.Pado);
    POLICY_NAME.put("disaggregation", PolicyType.Disaggregation);
  }
}
