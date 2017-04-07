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
package edu.snu.vortex.client;

import edu.snu.vortex.compiler.backend.Backend;
import edu.snu.vortex.compiler.backend.vortex.VortexBackend;
import edu.snu.vortex.compiler.frontend.Frontend;
import edu.snu.vortex.compiler.frontend.beam.BeamFrontend;
import edu.snu.vortex.compiler.ir.DAG;
import edu.snu.vortex.compiler.optimizer.Optimizer;
import edu.snu.vortex.runtime.common.plan.logical.ExecutionPlan;
import edu.snu.vortex.runtime.master.RuntimeMaster;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.snu.vortex.compiler.optimizer.Optimizer.POLICY_NAME;

/**
 * Job launcher.
 */
public final class JobLauncher {
  private static final Logger LOG = Logger.getLogger(JobLauncher.class.getName());

  private JobLauncher() {
  }

  public static void main(final String[] args) throws Exception {
    final String className = args[0];
    final String policyName = args[1];
    final String[] arguments = Arrays.copyOfRange(args, 2, args.length);

    final Frontend frontend = new BeamFrontend();
    final Optimizer optimizer = new Optimizer();
    final Backend<ExecutionPlan> backend = new VortexBackend();

    /**
     * Step 1: Compile
     */
    final DAG dag = frontend.compile(className, arguments); // TODO #30: Use Tang to Parse User Arguments
    LOG.log(Level.INFO, "##### VORTEX COMPILER (Before Optimization) #####");
    LOG.log(Level.INFO, dag.toString());

    final Optimizer.PolicyType optimizationPolicy = POLICY_NAME.get(policyName);
    final DAG optimizedDAG = optimizer.optimize(dag, optimizationPolicy);
    LOG.log(Level.INFO, "##### VORTEX COMPILER (After Optimization for " + optimizationPolicy + ") #####");
    LOG.log(Level.INFO, optimizedDAG.toString());

    final ExecutionPlan executionPlan = backend.compile(optimizedDAG);
    LOG.log(Level.INFO, "##### VORTEX COMPILER (After Compilation) #####");
    LOG.log(Level.INFO, executionPlan + "\n");

    /**
     * Step 2: Execute
     */
    LOG.log(Level.INFO, "##### VORTEX Runtime #####");
    new RuntimeMaster().execute(executionPlan);
  }
}
