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

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import edu.snu.nemo.common.exception.CompileTimeOptimizationException;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.compiler.optimizer.policy.Policy;
import edu.snu.nemo.conf.JobConf;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * An interface for optimizer, which manages the optimization over submitted IR DAGs through {@link Policy}s.
 * The instance of this class will reside in driver.
 */
public final class NemoOptimizer implements Optimizer {

  private final String dagDirectory;
  private final String optimizationPolicyCanonicalName;
  private final Injector injector;
  private final PubSubEventHandlerWrapper pubSubWrapper;

  @Inject
  private NemoOptimizer(@Parameter(JobConf.DAGDirectory.class) final String dagDirectory,
                        @Parameter(JobConf.OptimizationPolicy.class) final String optimizationPolicy,
                        final PubSubEventHandlerWrapper pubSubEventHandlerWrapper,
                        final Injector injector) {
    this.dagDirectory = dagDirectory;
    this.optimizationPolicyCanonicalName = optimizationPolicy;
    this.injector = injector;
    this.pubSubWrapper = pubSubEventHandlerWrapper;
  }

  /**
   * Optimize the submitted DAG.
   *
   * @param dag the input DAG to optimize.
   * @return optimized DAG, reshaped or tagged with execution properties.
   */
  @Override
  public DAG<IRVertex, IREdge> optimizeDag(final DAG<IRVertex, IREdge> dag) {
    try {
      dag.storeJSON(dagDirectory, "ir", "IR before optimization");
      final Policy optimizationPolicy = (Policy) Class.forName(optimizationPolicyCanonicalName).newInstance();

      if (optimizationPolicy == null) {
        throw new CompileTimeOptimizationException("A policy name should be specified.");
      }

      final DAG<IRVertex, IREdge> optimizedDAG = optimizationPolicy.runCompileTimeOptimization(dag, dagDirectory);
      optimizedDAG.storeJSON(dagDirectory, "ir-" + optimizationPolicy.getClass().getSimpleName(),
          "IR optimized for " + optimizationPolicy.getClass().getSimpleName());

      optimizationPolicy.registerRunTimeOptimizations(injector, pubSubWrapper);

      return optimizedDAG;
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }

  }
}
