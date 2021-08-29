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
package org.apache.nemo.client;

import org.apache.nemo.client.beam.NemoRunner;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.exception.InvalidUserMainException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.compiler.backend.Backend;
import org.apache.nemo.compiler.backend.nemo.NemoBackend;
import org.apache.nemo.compiler.backend.nemo.NemoPlanRewriter;
import org.apache.nemo.compiler.optimizer.NemoOptimizer;
import org.apache.nemo.compiler.optimizer.Optimizer;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.message.ClientRPC;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.local.LocalMessageEnvironment;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.PlanRewriter;
import org.apache.nemo.runtime.master.BroadcastManagerMaster;
import org.apache.nemo.runtime.master.scheduler.*;
import org.apache.reef.tang.*;
import org.apache.reef.tang.exceptions.InjectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Job launcher.
 */
public final class SimulatorLauncher extends Launcher {
  private static final Tang TANG = Tang.Factory.getTang();
  private static final Logger LOG = LoggerFactory.getLogger(SimulatorLauncher.class.getName());
  private static Configuration jobAndDriverConf = null;
  private static Configuration builtJobConf = null;
  private static PlanSimulator planSimulator;

  private static Optimizer optimizer = null;
  private static Backend<PhysicalPlan> backend = null;
  private static PlanRewriter planRewriter = null;

  /**
   * private construct or.
   */
  @Inject
  private SimulatorLauncher() {
    // empty
  }

  /**
   * Main JobLauncher method.
   *
   * @param args arguments.
   * @throws Exception exception on the way.
   */
  public static void main(final String[] args) throws Exception {
    try {
      LOG.info("start simulator launcher");
      setup(args);
      runUserProgramMain(builtJobConf);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
    LOG.info("end main");
  }

  /**
   * Set up the driver, etc. before the actual execution.
   *
   * @param args arguments.
   * @throws InjectionException     injection exception from REEF.
   * @throws ClassNotFoundException class not found exception.
   * @throws IOException            IO exception.
   */
  public static void setup(final String[] args)
    throws InjectionException, ClassNotFoundException, IOException, InvalidUserMainException {
    // Set launcher of NemoRunner
    NemoRunner.setJobLauncher(SimulatorLauncher.class);
    LOG.info("Project Root Path: {}", Util.fetchProjectRootPath());

    // Get Job and Driver Confs
    builtJobConf = getJobConf(args);
    validateJobConfig(builtJobConf);

    // Registers actions for launching the DAG.

    final Configuration driverMessageConfig = getDriverMessageConf();
    final String defaultExecutorResourceConfig = "[{\"type\":\"Transient\",\"memory_mb\":512,\"capacity\":5},"
      + "{\"type\":\"Reserved\",\"memory_mb\":512,\"capacity\":5}]";
    final Configuration executorResourceConfig = getJSONConf(builtJobConf, JobConf.ExecutorJSONPath.class,
      JobConf.ExecutorJSONContents.class, defaultExecutorResourceConfig);
    final Configuration bandwidthConfig = getJSONConf(builtJobConf, JobConf.BandwidthJSONPath.class,
      JobConf.BandwidthJSONContents.class, "");
    final Configuration nodeConfig = getJSONConf(builtJobConf, JobConf.NodeSpecJsonPath.class,
      JobConf.NodeSpecJsonContents.class, "");
    final Configuration schedulerConf = getSchedulerConf(builtJobConf);

    // Merge Job and Driver Confs
    jobAndDriverConf = Configurations.merge(builtJobConf, executorResourceConfig, driverMessageConfig,
      bandwidthConfig, schedulerConf, nodeConfig);

    Injector injector = TANG.newInjector(jobAndDriverConf);

    optimizer = injector.getInstance(NemoOptimizer.class);
    backend = injector.getInstance(NemoBackend.class);
    planRewriter = injector.getInstance(NemoPlanRewriter.class);

    planSimulator = injector.getInstance(StreamingPlanSimulator.class);


    // Start Driver and launch user program.
    if (jobAndDriverConf == null) {
      throw new RuntimeException("Configuration for launching driver is not ready");
    }
  }

  public static Configuration getSchedulerConf(final Configuration jobConf)
    throws InjectionException {
    final Injector injector = TANG.newInjector(jobConf);
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    String classImplName = injector.getNamedInstance(JobConf.SchedulerImplClassName.class);
    if (classImplName.equals(StreamingScheduler.class.getName())) {
      jcb.bindImplementation(ScheduleSimulator.class, StreamingScheduleSimulator.class);
      jcb.bindImplementation(PlanSimulator.class, StreamingPlanSimulator.class);
    } else {
      throw new RuntimeException("batch simulator is not ready");
    }
    jcb.bindImplementation(MessageEnvironment.class, LocalMessageEnvironment.class);
    jcb.bindImplementation(ClientRPC.class, ClientRPCSimulator.class);
    jcb.bindImplementation(PlanRewriter.class, NemoPlanRewriter.class);
    return jcb.build();
  }

  /**
   * Launch application using the application DAG.
   * Notice that we launch the DAG one at a time, as the result of a DAG has to be immediately returned to the
   * Java variable before the application can be resumed.
   *
   * @param dag the application DAG.
   */
  // When modifying the signature of this method, see CompilerTestUtil#compileDAG and make corresponding changes
  public static void launchDAG(final IRDAG dag) {
    launchDAG(dag, Collections.emptyMap(), "");
  }

  /**
   * @param dag   the application DAG.
   * @param jobId job ID.
   */
  public static void launchDAG(final IRDAG dag, final String jobId) {
    launchDAG(dag, Collections.emptyMap(), jobId);
  }

  /**
   * @param dag                the application DAG.
   * @param broadcastVariables broadcast variables (can be empty).
   * @param jobId              job ID.
   */
  public static void launchDAG(final IRDAG dag,
                               final Map<Serializable, Object> broadcastVariables,
                               final String jobId) {
    // launch driver if it hasn't been already
    LOG.info("Launching DAG...");
    try {
      final IRDAG optimizedDAG = optimizer.optimizeAtCompileTime(dag);
      ((NemoPlanRewriter) planRewriter).setCurrentIRDAG(optimizedDAG);
      final PhysicalPlan physicalPlan = backend.compile(optimizedDAG);

      ((NemoPlanRewriter) planRewriter).setCurrentPhysicalPlan(physicalPlan);
      BroadcastManagerMaster.registerBroadcastVariablesFromClient(broadcastVariables);

      planSimulator.simulate(physicalPlan, 2);
      LOG.info("end execution");
      planSimulator.terminate();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}

