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
package org.apache.nemo.driver;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.backend.Backend;
import org.apache.nemo.compiler.optimizer.Optimizer;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.master.PlanStateManager;
import org.apache.nemo.runtime.master.RuntimeMaster;
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

  private final int maxScheduleAttempt;

  private final RuntimeMaster runtimeMaster;
  private final Optimizer optimizer;
  private final Backend<PhysicalPlan> backend;

  @Inject
  private UserApplicationRunner(@Parameter(JobConf.MaxTaskAttempt.class) final int maxScheduleAttempt,
                                final Optimizer optimizer,
                                final Backend<PhysicalPlan> backend,
                                final RuntimeMaster runtimeMaster) {
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

      // Execute!
      final Pair<PlanStateManager, ScheduledExecutorService> executionResult =
          runtimeMaster.execute(physicalPlan, maxScheduleAttempt);

      // Wait for the job to finish and stop logging
      final PlanStateManager planStateManager = executionResult.left();
      final ScheduledExecutorService dagLoggingExecutor = executionResult.right();
      try {
        planStateManager.waitUntilFinish();
        dagLoggingExecutor.shutdown();
      } finally {
        planStateManager.storeJSON("final");
      }

      LOG.info("{} is complete!", physicalPlan.getPlanId());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}
