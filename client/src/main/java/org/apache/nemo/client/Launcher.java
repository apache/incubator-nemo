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

import com.google.common.annotations.VisibleForTesting;
import org.apache.nemo.common.exception.InvalidUserMainException;
import org.apache.nemo.compiler.backend.nemo.NemoPlanRewriter;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.driver.NemoDriver;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageParameters;
import org.apache.nemo.runtime.common.plan.PlanRewriter;
import org.apache.nemo.runtime.master.scheduler.Scheduler;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.parameters.JobMessageHandler;
import org.apache.reef.io.network.naming.LocalNameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.IdentifierFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Job launcher.
 */
public abstract class Launcher {

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
  private static final Logger LOG = LoggerFactory.getLogger(Launcher.class.getName());
  private static final int LOCAL_NUMBER_OF_EVALUATORS = 100; // hopefully large enough for our use....

  private static final String[] EMPTY_USER_ARGS = new String[0];

  /**
   * Validate the configuration of the application's main method.
   * @param jobConf Configuration of the application.
   * @throws InvalidUserMainException when the user main is invalid (e.g., non-existing class/method).
   */
  static void validateJobConfig(final Configuration jobConf) throws InvalidUserMainException {
    final Injector injector = TANG.newInjector(jobConf);
    try {
      final String className = injector.getNamedInstance(JobConf.UserMainClass.class);
      final Class userCode = Class.forName(className);
      final Method method = userCode.getMethod("main", String[].class);
      if (!Modifier.isStatic(method.getModifiers())) {
        throw new InvalidUserMainException("User Main Method not static");
      }
      if (!Modifier.isPublic(userCode.getModifiers())) {
        throw new InvalidUserMainException("User Main Class not public");
      }

    } catch (final InjectionException | ClassNotFoundException | NoSuchMethodException e) {
      throw new InvalidUserMainException(e);
    }
  }

  /**
   * Run user-provided main method.
   *
   * @param jobConf the job configuration
   * @throws Exception on any exceptions on the way
   */
  static void runUserProgramMain(final Configuration jobConf) throws Exception {
    final Injector injector = TANG.newInjector(jobConf);
    final String className = injector.getNamedInstance(JobConf.UserMainClass.class);
    final String userArgsString = injector.getNamedInstance(JobConf.UserMainArguments.class);
    final String[] args = userArgsString.isEmpty() ? EMPTY_USER_ARGS : userArgsString.split(" ");
    final Class userCode = Class.forName(className);
    final Method method = userCode.getMethod("main", String[].class);

    LOG.info("User program started");
    method.invoke(null, (Object) args);
    LOG.info("User program finished");
  }

  /**
   * @return client configuration.
   */
  static Configuration getClientConf() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(JobMessageHandler.class, NemoClient.JobMessageHandler.class);
    return jcb.build();
  }

  /**
   * Fetch scheduler configuration.
   *
   * @param jobConf job configuration.
   * @return the scheduler configuration.
   * @throws ClassNotFoundException exception while finding the class.
   * @throws InjectionException     exception while injection (REEF Tang).
   */
  static Configuration getSchedulerConf(final Configuration jobConf)
    throws ClassNotFoundException, InjectionException {
    final Injector injector = TANG.newInjector(jobConf);
    final String classImplName = injector.getNamedInstance(JobConf.SchedulerImplClassName.class);
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final Class schedulerImpl = ((Class<Scheduler>) Class.forName(classImplName));
    jcb.bindImplementation(Scheduler.class, schedulerImpl);
    jcb.bindImplementation(PlanRewriter.class, NemoPlanRewriter.class);
    return jcb.build();
  }

  /**
   * Get driver ncs configuration.
   *
   * @return driver ncs configuration.
   */
  static Configuration getDriverNcsConf() {
    return Configurations.merge(NameServerConfiguration.CONF.build(),
      LocalNameResolverConfiguration.CONF.build(),
      TANG.newConfigurationBuilder()
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build());
  }

  /**
   * Get driver message configuration.
   *
   * @return driver message configuration.
   */
  static Configuration getDriverMessageConf() {
    return TANG.newConfigurationBuilder()
      .bindNamedParameter(MessageParameters.SenderId.class, MessageEnvironment.MASTER_COMMUNICATION_ID)
      .build();
  }

  /**
   * Get driver configuration.
   *
   * @param jobConf job Configuration to get job id and driver memory.
   * @return driver configuration.
   * @throws InjectionException exception while injection.
   */
  static Configuration getDriverConf(final Configuration jobConf) throws InjectionException {
    final Injector injector = TANG.newInjector(jobConf);
    final String jobId = injector.getNamedInstance(JobConf.JobId.class);
    final int driverMemory = injector.getNamedInstance(JobConf.DriverMemMb.class);
    return DriverConfiguration.CONF
      .setMultiple(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getAllClasspathJars())
      .set(DriverConfiguration.ON_DRIVER_STARTED, NemoDriver.StartHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, NemoDriver.AllocatedEvaluatorHandler.class)
      .set(DriverConfiguration.ON_CONTEXT_ACTIVE, NemoDriver.ActiveContextHandler.class)
      .set(DriverConfiguration.ON_EVALUATOR_FAILED, NemoDriver.FailedEvaluatorHandler.class)
      .set(DriverConfiguration.ON_CONTEXT_FAILED, NemoDriver.FailedContextHandler.class)
      .set(DriverConfiguration.ON_DRIVER_STOP, NemoDriver.DriverStopHandler.class)
      .set(DriverConfiguration.DRIVER_IDENTIFIER, jobId)
      .set(DriverConfiguration.DRIVER_MEMORY, driverMemory)
      .build();
  }

  /**
   * Get job configuration.
   *
   * @param args arguments to be processed as command line.
   * @return job configuration.
   * @throws IOException        exception while processing command line.
   */
  @VisibleForTesting
  public static Configuration getJobConf(final String[] args) throws IOException {
    final JavaConfigurationBuilder confBuilder = TANG.newConfigurationBuilder();
    final CommandLine cl = new CommandLine(confBuilder);
    cl.registerShortNameOfClass(JobConf.JobId.class);
    cl.registerShortNameOfClass(JobConf.UserMainClass.class);
    cl.registerShortNameOfClass(JobConf.UserMainArguments.class);
    cl.registerShortNameOfClass(JobConf.DAGDirectory.class);
    cl.registerShortNameOfClass(JobConf.OptimizationPolicy.class);
    cl.registerShortNameOfClass(JobConf.DeployMode.class);
    cl.registerShortNameOfClass(JobConf.ExecutorType.class);
    cl.registerShortNameOfClass(JobConf.DriverMemMb.class);
    cl.registerShortNameOfClass(JobConf.ExecutorJSONPath.class);
    cl.registerShortNameOfClass(JobConf.BandwidthJSONPath.class);
    cl.registerShortNameOfClass(JobConf.NodeSpecJsonPath.class);
    cl.registerShortNameOfClass(JobConf.JVMHeapSlack.class);
    cl.registerShortNameOfClass(JobConf.IORequestHandleThreadsTotal.class);
    cl.registerShortNameOfClass(JobConf.MaxTaskAttempt.class);
    cl.registerShortNameOfClass(JobConf.FileDirectory.class);
    cl.registerShortNameOfClass(JobConf.GlusterVolumeDirectory.class);
    cl.registerShortNameOfClass(JobConf.PartitionTransportServerPort.class);
    cl.registerShortNameOfClass(JobConf.PartitionTransportServerBacklog.class);
    cl.registerShortNameOfClass(JobConf.PartitionTransportServerNumListeningThreads.class);
    cl.registerShortNameOfClass(JobConf.PartitionTransportServerNumWorkingThreads.class);
    cl.registerShortNameOfClass(JobConf.PartitionTransportClientNumThreads.class);
    cl.registerShortNameOfClass(JobConf.MaxNumDownloadsForARuntimeEdge.class);
    cl.registerShortNameOfClass(JobConf.SchedulerImplClassName.class);
    cl.registerShortNameOfClass(JobConf.ScheduleSerThread.class);
    cl.registerShortNameOfClass(JobConf.MaxOffheapRatio.class);
    cl.registerShortNameOfClass(JobConf.ChunkSizeKb.class);
    cl.processCommandLine(args);
    return confBuilder.build();
  }

  /**
   * Get deploy mode configuration.
   *
   * @param jobConf job configuration to get deploy mode.
   * @return deploy mode configuration.
   * @throws InjectionException exception while injection.
   */
  static Configuration getDeployModeConf(final Configuration jobConf) throws InjectionException {
    final Injector injector = TANG.newInjector(jobConf);
    final String deployMode = injector.getNamedInstance(JobConf.DeployMode.class);
    switch (deployMode) {
      case "local":
        return LocalRuntimeConfiguration.CONF
          .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, LOCAL_NUMBER_OF_EVALUATORS)
          .build();
      case "yarn":
        return YarnClientConfiguration.CONF
          .set(YarnClientConfiguration.JVM_HEAP_SLACK, injector.getNamedInstance(JobConf.JVMHeapSlack.class)
            + injector.getNamedInstance(JobConf.MaxOffheapRatio.class))
          // Off-heap memory size is added to memory slack so that JVM heap region does not invade the off-heap region.
          .build();
      default:
        throw new UnsupportedOperationException(deployMode);
    }
  }

  /**
   * Read json file and return its contents as configuration parameter.
   *
   * @param jobConf           job configuration to get json path.
   * @param pathParameter     named parameter represents path to the json file, or an empty string
   * @param contentsParameter named parameter represents contents of the file
   * @param defaultContent    the default configuration
   * @return configuration with contents of the file, or an empty string as value for {@code contentsParameter}
   * @throws InjectionException exception while injection.
   */
  static Configuration getJSONConf(final Configuration jobConf,
                                   final Class<? extends Name<String>> pathParameter,
                                   final Class<? extends Name<String>> contentsParameter,
                                   final String defaultContent)
    throws InjectionException {
    final Injector injector = TANG.newInjector(jobConf);
    try {
      final String path = injector.getNamedInstance(pathParameter);
      final String contents = path.isEmpty() ? defaultContent
        : new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
      return TANG.newConfigurationBuilder()
        .bindNamedParameter(contentsParameter, contents)
        .build();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
