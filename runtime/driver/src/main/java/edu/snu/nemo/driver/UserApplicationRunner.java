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
package edu.snu.nemo.driver;

import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.eventhandler.PubSubEventHandlerWrapper;
import edu.snu.nemo.common.eventhandler.RuntimeEventHandler;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.compiler.backend.Backend;
import edu.snu.nemo.compiler.backend.nemo.NemoBackend;
import edu.snu.nemo.compiler.optimizer.CompiletimeOptimizer;
import edu.snu.nemo.compiler.optimizer.policy.Policy;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.runtime.common.plan.physical.PhysicalPlan;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.master.RuntimeMaster;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Base64;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Compiles and runs User application.
 */
public final class UserApplicationRunner implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(UserApplicationRunner.class.getName());

  private final String dagDirectory;
  private final String dagString;
  private final String optimizationPolicyCanonicalName;
  private final int maxScheduleAttempt;

  private final Injector injector;
  private final RuntimeMaster runtimeMaster;
  private final Backend<PhysicalPlan> backend;

  private final PubSubEventHandlerWrapper pubSubWrapper;

  @Inject
  private UserApplicationRunner(@Parameter(JobConf.DAGDirectory.class) final String dagDirectory,
                                @Parameter(JobConf.SerializedDAG.class) final String dagString,
                                @Parameter(JobConf.OptimizationPolicy.class) final String optimizationPolicy,
                                @Parameter(JobConf.MaxScheduleAttempt.class) final int maxScheduleAttempt,
                                final PubSubEventHandlerWrapper pubSubEventHandlerWrapper,
                                final Injector injector,
                                final RuntimeMaster runtimeMaster) {
    this.dagDirectory = dagDirectory;
    this.dagString = dagString;
    this.optimizationPolicyCanonicalName = optimizationPolicy;
    this.maxScheduleAttempt = maxScheduleAttempt;
    this.injector = injector;
    this.runtimeMaster = runtimeMaster;
    this.backend = new NemoBackend();
    this.pubSubWrapper = pubSubEventHandlerWrapper;
  }

  @Override
  public void run() {
    try {
      LOG.info("##### Nemo Compiler #####");

      final DAG<IRVertex, IREdge> dag = SerializationUtils.deserialize(Base64.getDecoder().decode(dagString));
      dag.storeJSON(dagDirectory, "ir", "IR before optimization");
      final Policy optimizationPolicy = (Policy) Class.forName(optimizationPolicyCanonicalName).newInstance();

      final DAG<IRVertex, IREdge> optimizedDAG = CompiletimeOptimizer.optimize(dag, optimizationPolicy, dagDirectory);
      optimizedDAG.storeJSON(dagDirectory, "ir-" + optimizationPolicy.getClass().getSimpleName(),
          "IR optimized for " + optimizationPolicy.getClass().getSimpleName());

      optimizationPolicy.getRuntimePasses().forEach(runtimePass ->
          runtimePass.getEventHandlerClasses().forEach(runtimeEventHandlerClass -> {
            try {
              final RuntimeEventHandler runtimeEventHandler = injector.getInstance(runtimeEventHandlerClass);
              pubSubWrapper.getPubSubEventHandler()
                  .subscribe(runtimeEventHandler.getEventClass(), runtimeEventHandler);
            } catch (final Exception e) {
              throw new RuntimeException(e);
            }
          }));

      final PhysicalPlan physicalPlan = backend.compile(optimizedDAG);

      physicalPlan.getStageDAG().storeJSON(dagDirectory, "plan", "physical execution plan by compiler");

      // Execute!
      final Pair<JobStateManager, ScheduledExecutorService> executionResult =
          runtimeMaster.execute(physicalPlan, maxScheduleAttempt);

      // Wait for the job to finish and stop logging
      final JobStateManager jobStateManager = executionResult.left();
      final ScheduledExecutorService dagLoggingExecutor = executionResult.right();
      jobStateManager.waitUntilFinish();
      dagLoggingExecutor.shutdown();

      jobStateManager.storeJSON(dagDirectory, "final");
      LOG.info("{} is complete!", physicalPlan.getId());
      runtimeMaster.terminate();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}
