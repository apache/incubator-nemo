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

import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.compiler.backend.Backend;
import org.apache.nemo.compiler.backend.nemo.NemoPlanRewriter;
import org.apache.nemo.compiler.optimizer.Optimizer;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.metric.JobMetric;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.PlanRewriter;
import org.apache.nemo.runtime.master.PlanStateManager;
import org.apache.nemo.runtime.master.RuntimeMaster;
import org.apache.nemo.runtime.master.metric.MetricStore;
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
  private final PlanRewriter planRewriter;

  /**
   * Constructor.
   *
   * @param maxScheduleAttempt maximum scheuling attempt.
   * @param optimizer          the nemo optimizer.
   * @param backend            the backend to actually execute the job.
   * @param runtimeMaster      the runtime master.
   * @param planRewriter       plan rewriter
   */
  @Inject
  private UserApplicationRunner(@Parameter(JobConf.MaxTaskAttempt.class) final int maxScheduleAttempt,
                                final Optimizer optimizer,
                                final Backend<PhysicalPlan> backend,
                                final RuntimeMaster runtimeMaster,
                                final PlanRewriter planRewriter) {
    this.maxScheduleAttempt = maxScheduleAttempt;
    this.runtimeMaster = runtimeMaster;
    this.optimizer = optimizer;
    this.backend = backend;
    this.planRewriter = planRewriter;
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
      final long startTime = System.currentTimeMillis();
      LOG.info("##### Nemo Compiler Start #####");

      final IRDAG dag = SerializationUtils.deserialize(Base64.getDecoder().decode(dagString));
      final IRDAG optimizedDAG = optimizer.optimizeAtCompileTime(dag);
      ((NemoPlanRewriter) planRewriter).setCurrentIRDAG(optimizedDAG);
      final PhysicalPlan physicalPlan = backend.compile(optimizedDAG);
      ((NemoPlanRewriter) planRewriter).setCurrentPhysicalPlan(physicalPlan);
      LOG.info("##### Nemo Compiler Finish #####");

      // Execute!
      final Pair<PlanStateManager, ScheduledExecutorService> executionResult =
        runtimeMaster.execute(physicalPlan, maxScheduleAttempt);
      runtimeMaster.recordIRDAGMetrics(optimizedDAG, physicalPlan.getPlanId());

      // Wait for the job to finish and stop logging
      final PlanStateManager planStateManager = executionResult.left();
      final ScheduledExecutorService dagLoggingExecutor = executionResult.right();
      try {
        planStateManager.waitUntilFinish();
        dagLoggingExecutor.shutdown();
      } finally {
        planStateManager.storeJSON("final");
      }

      final long endTime = System.currentTimeMillis();
      LOG.info("{} is complete!", physicalPlan.getPlanId());
      MetricStore.getStore().getOrCreateMetric(JobMetric.class, physicalPlan.getPlanId())
        .setJobDuration(endTime - startTime);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}
