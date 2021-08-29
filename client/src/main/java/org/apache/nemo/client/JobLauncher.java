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

import com.google.protobuf.ByteString;
import org.apache.nemo.common.exception.InvalidUserMainException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nemo.common.Util;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.tang.*;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Job launcher.
 */
public final class JobLauncher extends Launcher {

  static {
    System.out.println(
      "\nPowered by\n"
        + "    _   __                   \n"
        + "   / | / /__  ____ ___  ____ \n"
        + "  /  |/ / _ \\/ __ `__ \\/ __ \\\n"
        + " / /|  /  __/ / / / / / /_/ /\n"
        + "/_/ |_/\\___/_/ /_/ /_/\\____/ \n"
    );
  }

  private static final Tang TANG = Tang.Factory.getTang();
  private static final Logger LOG = LoggerFactory.getLogger(JobLauncher.class.getName());
  private static Configuration jobAndDriverConf = null;
  private static Configuration deployModeConf = null;
  private static Configuration builtJobConf = null;

  private static DriverLauncher driverLauncher;
  private static DriverRPCServer driverRPCServer;

  private static boolean isSetUp = false;
  private static CountDownLatch driverReadyLatch;
  private static CountDownLatch jobDoneLatch;
  private static String serializedDAG;
  private static final List<?> COLLECTED_DATA = new ArrayList<>();

  /**
   * private constructor.
   */
  private JobLauncher() {
  }

  /**
   * Main JobLauncher method.
   *
   * @param args arguments.
   * @throws Exception exception on the way.
   */
  public static void main(final String[] args) throws Exception {
    try {
      setup(args);
      // Launch client main. The shutdown() method is called inside the launchDAG() method.
      runUserProgramMain(builtJobConf);
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
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
    // Get Job and Driver Confs
    LOG.info("Project Root Path: {}", Util.fetchProjectRootPath());
    builtJobConf = getJobConf(args);
    validateJobConfig(builtJobConf);

    // Registers actions for launching the DAG.
    LOG.info("Launching RPC Server");
    driverRPCServer = new DriverRPCServer();
    driverRPCServer
      .registerHandler(ControlMessage.DriverToClientMessageType.DriverStarted, event -> {
      })
      .registerHandler(ControlMessage.DriverToClientMessageType.DriverReady, event -> driverReadyLatch.countDown())
      .registerHandler(ControlMessage.DriverToClientMessageType.ExecutionDone, event -> jobDoneLatch.countDown())
      .registerHandler(ControlMessage.DriverToClientMessageType.DataCollected, message -> COLLECTED_DATA.addAll(
        SerializationUtils.deserialize(Base64.getDecoder().decode(message.getDataCollected().getData()))))
      .registerHandler(ControlMessage.DriverToClientMessageType.LaunchOptimization, message ->
        ClientUtils.handleOptimizationType(message, driverRPCServer))
      .run();

    final Configuration driverConf = getDriverConf(builtJobConf);
    final Configuration driverNcsConf = getDriverNcsConf();
    final Configuration driverMessageConfig = getDriverMessageConf();
    final String defaultExecutorResourceConfig = "[{\"type\":\"Transient\",\"memory_mb\":512,\"capacity\":5},"
      + "{\"type\":\"Reserved\",\"memory_mb\":512,\"capacity\":5}]";
    final Configuration executorResourceConfig = getJSONConf(builtJobConf, JobConf.ExecutorJSONPath.class,
      JobConf.ExecutorJSONContents.class, defaultExecutorResourceConfig);
    final Configuration bandwidthConfig = getJSONConf(builtJobConf, JobConf.BandwidthJSONPath.class,
      JobConf.BandwidthJSONContents.class, "");
    final Configuration clientConf = getClientConf();
    final Configuration schedulerConf = getSchedulerConf(builtJobConf);

    // Merge Job and Driver Confs
    jobAndDriverConf = Configurations.merge(builtJobConf, driverConf, driverNcsConf, driverMessageConfig,
      executorResourceConfig, bandwidthConfig, driverRPCServer.getListeningConfiguration(), schedulerConf);

    // Get DeployMode Conf
    deployModeConf = Configurations.merge(getDeployModeConf(builtJobConf), clientConf);

    // Start Driver and launch user program.
    if (jobAndDriverConf == null || deployModeConf == null || builtJobConf == null) {
      throw new RuntimeException("Configuration for launching driver is not ready");
    }

    // Launch driver
    LOG.info("Launching driver");
    driverReadyLatch = new CountDownLatch(1);
    jobDoneLatch = new CountDownLatch(1);
    isSetUp = true;
    driverLauncher = DriverLauncher.getLauncher(deployModeConf);
    driverLauncher.submit(jobAndDriverConf, 500);
    // When the driver is up and the resource is ready, the DriverReady message is delivered.
  }

  /**
   * Clean up everything.
   */
  private static final String INTERRUPTED = "Interrupted: ";
  public static void shutdown() {
    // Trigger driver shutdown afterwards
    driverRPCServer.send(ControlMessage.ClientToDriverMessage.newBuilder()
      .setType(ControlMessage.ClientToDriverMessageType.DriverShutdown).build());
    // Wait for driver to naturally finish
    synchronized (driverLauncher) {
      while (!driverLauncher.getStatus().isDone()) {
        try {
          LOG.info("Wait for the driver to finish");
          driverLauncher.wait();
        } catch (final InterruptedException e) {
          LOG.warn(INTERRUPTED, e);
          // clean up state...
          Thread.currentThread().interrupt();
        }
      }
      LOG.info("Driver terminated");
    }

    // Close everything that's left
    driverRPCServer.shutdown();
    driverLauncher.close();
    isSetUp = false;
    final Optional<Throwable> possibleError = driverLauncher.getStatus().getError();
    if (possibleError.isPresent()) {
      throw new RuntimeException(possibleError.get());
    } else if (jobDoneLatch.getCount() > 0) {
      LOG.info("Job cancelled");
    } else {
      LOG.info("Job successfully completed");
    }
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
    if (!isSetUp) {
      try {
        setup(new String[]{"-job_id", jobId});
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    // Wait until the driver is ready.
    try {
      LOG.info("Waiting for the driver to be ready");
      driverReadyLatch.await();
    } catch (final InterruptedException e) {
      LOG.warn(INTERRUPTED, e);
      // clean up state...
      Thread.currentThread().interrupt();
    }

    LOG.info("Launching DAG...");
    serializedDAG = Base64.getEncoder().encodeToString(SerializationUtils.serialize(dag));
    if (jobDoneLatch.getCount() == 0) {  // when this is not the first execution.
      jobDoneLatch = new CountDownLatch(1);
    }

    driverRPCServer.send(ControlMessage.ClientToDriverMessage.newBuilder()
      .setType(ControlMessage.ClientToDriverMessageType.LaunchDAG)
      .setLaunchDAG(ControlMessage.LaunchDAGMessage.newBuilder()
        .setDag(serializedDAG)
        .setBroadcastVars(ByteString.copyFrom(SerializationUtils.serialize((Serializable) broadcastVariables)))
        .build())
      .build());

    // Wait for the ExecutionDone message from the driver
    try {
      LOG.info("Waiting for the DAG to finish execution");
      jobDoneLatch.await();
    } catch (final InterruptedException e) {
      LOG.warn(INTERRUPTED, e);
      // clean up state...
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } finally {
      LOG.info("DAG execution done");
      // trigger shutdown.
      shutdown();
    }
  }

  /**
   * Get the built job configuration.
   * It can be {@code null} if this method is not called by the process which called the main function of this class.
   *
   * @return the built job configuration.
   */
  public static Configuration getBuiltJobConf() {
    return builtJobConf;
  }

  /**
   * Get the collected data.
   *
   * @param <T> the type of the data.
   * @return the collected data.
   */
  public static <T> List<T> getCollectedData() {
    final List<T> result = (List<T>) new ArrayList<>(COLLECTED_DATA);
    COLLECTED_DATA.clear(); // flush after fetching.
    return result;
  }
}
