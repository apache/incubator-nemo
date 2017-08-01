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
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.compiler.backend.Backend;
import edu.snu.vortex.compiler.backend.vortex.VortexBackend;
import edu.snu.vortex.compiler.frontend.Frontend;
import edu.snu.vortex.compiler.frontend.beam.BeamFrontend;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.optimizer.Optimizer;
import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.runtime.common.plan.physical.PhysicalPlan;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.vortex.compiler.optimizer.Optimizer.POLICY_NAME;

/**
 * Compiles and runs User application.
 */
public final class UserApplicationRunner implements Runnable {
  private static final Logger LOG = Logger.getLogger(UserApplicationRunner.class.getName());

  private final String dagDirectory;
  private final String className;
  private final String[] arguments;
  private final String policyName;

  private final RuntimeMaster runtimeMaster;
  private final Frontend frontend;
  private final Optimizer optimizer;
  private final Backend<PhysicalPlan> backend;

  @Inject
  private UserApplicationRunner(@Parameter(JobConf.DAGDirectory.class) final String dagDirectory,
                                @Parameter(JobConf.UserMainClass.class) final String className,
                                @Parameter(JobConf.UserMainArguments.class) final String arguments,
                                @Parameter(JobConf.OptimizationPolicy.class) final String policyName,
                                final RuntimeMaster runtimeMaster) {
    this.dagDirectory = dagDirectory;
    this.className = className;
    this.arguments = arguments.split(" ");
    this.policyName = policyName;
    this.runtimeMaster = runtimeMaster;
    this.frontend = new BeamFrontend();
    this.optimizer = new Optimizer();
    this.backend = new VortexBackend();
  }

  @Override
  public void run() {
    try {
      LOG.log(Level.INFO, "##### VORTEX Compiler #####");
      final DAG<IRVertex, IREdge> dag = frontend.compile(className, arguments);
      dag.storeJSON(dagDirectory, "ir", "IR before optimization");

      final Optimizer.PolicyType optimizationPolicy = POLICY_NAME.get(policyName);
      final DAG<IRVertex, IREdge> optimizedDAG = optimizer.optimize(dag, optimizationPolicy, dagDirectory);
      optimizedDAG.storeJSON(dagDirectory, "ir-" + optimizationPolicy, "IR optimized for " + optimizationPolicy);

      final PhysicalPlan physicalPlan = backend.compile(optimizedDAG);

      physicalPlan.getStageDAG().storeJSON(dagDirectory, "plan", "physical execution plan by compiler");
      runtimeMaster.execute(physicalPlan, frontend.getClientEndpoint());
      runtimeMaster.terminate();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
