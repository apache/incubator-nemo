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
import edu.snu.vortex.compiler.optimizer.Optimizer;
import edu.snu.vortex.runtime.common.plan.logical.ExecutionPlan;
import edu.snu.vortex.runtime.master.RuntimeMaster;
import edu.snu.vortex.utils.dag.DAG;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;

import java.io.IOException;
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
    final Configuration configuration = getJobConf(args);
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);

    final Frontend frontend = new BeamFrontend();
    final Optimizer optimizer = new Optimizer();
    final Backend<ExecutionPlan> backend = new VortexBackend();

    /**
     * Step 1: Compile
     */
    final String className = injector.getNamedInstance(JobConf.UserMainClass.class);
    final String[] arguments = injector.getNamedInstance(JobConf.UserMainArguments.class).split(" ");
    final DAG dag = frontend.compile(className, arguments);
    LOG.log(Level.INFO, "##### VORTEX COMPILER (Before Optimization) #####");
    LOG.log(Level.INFO, dag.toString());

    final String policyName = injector.getNamedInstance(JobConf.OptimizationPolicy.class);
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

  public static Configuration getJobConf(final String[] args) throws IOException, InjectionException {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine cl = new CommandLine(confBuilder);
    cl.registerShortNameOfClass(JobConf.UserMainClass.class);
    cl.registerShortNameOfClass(JobConf.UserMainArguments.class);
    cl.registerShortNameOfClass(JobConf.OptimizationPolicy.class);
    cl.processCommandLine(args);
    return confBuilder.build();
  }
}
