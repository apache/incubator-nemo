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
package edu.snu.nemo.driver;

import edu.snu.nemo.common.Pair;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.exception.CompileTimeOptimizationException;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.compiler.backend.Backend;
import edu.snu.nemo.compiler.optimizer.Optimizer;
import edu.snu.nemo.compiler.optimizer.policy.Policy;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.runtime.common.plan.PhysicalPlan;
import edu.snu.nemo.runtime.master.PlanStateManager;
import edu.snu.nemo.runtime.master.RuntimeMaster;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Base64;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Compiles and runs User application.
 */
public final class UserApplicationRunner {
  private static final Logger LOG = LoggerFactory.getLogger(UserApplicationRunner.class.getName());

  private final String dagDirectory;
  private final int maxScheduleAttempt;

  private final RuntimeMaster runtimeMaster;
  private final Optimizer optimizer;
  private final Backend<PhysicalPlan> backend;

  @Inject
  private UserApplicationRunner(@Parameter(JobConf.DAGDirectory.class) final String dagDirectory,
                                @Parameter(JobConf.MaxTaskAttempt.class) final int maxScheduleAttempt,
                                final Optimizer optimizer,
                                final Backend<PhysicalPlan> backend,
                                final RuntimeMaster runtimeMaster) {
    this.dagDirectory = dagDirectory;
    this.maxScheduleAttempt = maxScheduleAttempt;
    this.runtimeMaster = runtimeMaster;
    this.optimizer = optimizer;
    this.backend = backend;
  }

  /**
   * Run the user program submitted by Nemo Client.
   * Specifically, deserialize DAG from Client, optimize it, generate physical plan,
   * and tell {@link RuntimeMaster} to execute the plan.
   *
   * @param dagString Serialized IR DAG from Nemo Client.
   */
  public synchronized void run(final String dagString) {
    try {
      LOG.info("##### Nemo Compiler Start #####");

      final DAG<IRVertex, IREdge> dag = SerializationUtils.deserialize(Base64.getDecoder().decode(dagString));
      final DAG<IRVertex, IREdge> optimizedDAG = optimizer.optimizeDag(dag);
      final PhysicalPlan physicalPlan = backend.compile(optimizedDAG);

      LOG.info("##### Nemo Compiler Finish #####");

      physicalPlan.getStageDAG().storeJSON(dagDirectory, "plan", "physical execution plan by compiler");

      // Execute!
      final Pair<PlanStateManager, ScheduledExecutorService> executionResult =
          runtimeMaster.execute(physicalPlan, maxScheduleAttempt);

      // Wait for the job to finish and stop logging
      final PlanStateManager planStateManager = executionResult.left();
      final ScheduledExecutorService dagLoggingExecutor = executionResult.right();
      planStateManager.waitUntilFinish();
      dagLoggingExecutor.shutdown();

      planStateManager.storeJSON(dagDirectory, "final");
      LOG.info("{} is complete!", physicalPlan.getId());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}
