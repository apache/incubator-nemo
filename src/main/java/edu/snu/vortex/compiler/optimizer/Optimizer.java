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

import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.optimizer.passes.*;

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
  public DAG optimize(final DAG dag, final PolicyType policyType) throws Exception {
    final Policy policy = new Policy(POLICIES.get(policyType));
    return policy.process(dag);
  }

  /**
   * Policy class.
   * It runs a list of passes sequentially to optimize the DAG.
   */
  private static final class Policy {
    private final List<Pass> passes;

    private Policy(final List<Pass> passes) {
      if (passes.isEmpty()) {
        throw new NoSuchElementException("No instantiation pass supplied to the policy!");
      }
      this.passes = passes;
    }

    private DAG process(final DAG dag) throws Exception {
      DAG optimizedDAG = dag;
      passes.forEach(pass -> {
        try {
          pass.process(optimizedDAG);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      return optimizedDAG;
    }
  }

  /**
   * Enum for different types of instantiation policies.
   */
  public enum PolicyType {
    Pado,
    Disaggregation,
  }

  /**
   * A HashMap to match each of instantiation policies with a combination of instantiation passes.
   */
  private static final Map<PolicyType, List<Pass>> POLICIES = new HashMap<>();
  static {
    POLICIES.put(PolicyType.Pado,
        Arrays.asList(new PadoOperatorPass(), new PadoEdgePass()));
    POLICIES.put(PolicyType.Disaggregation,
        Arrays.asList(new DisaggregationPass()));
  }
}
