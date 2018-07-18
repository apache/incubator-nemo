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
package edu.snu.nemo.client;

import com.google.common.annotations.VisibleForTesting;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.driver.NemoDriver;
import edu.snu.nemo.runtime.common.comm.ControlMessage;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.MessageParameters;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
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
import org.apache.reef.util.Optional;
import org.apache.reef.wake.IdentifierFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Job launcher.
 */
public final class JobLauncher {
  private static final Tang TANG = Tang.Factory.getTang();
  private static final Logger LOG = LoggerFactory.getLogger(JobLauncher.class.getName());
  private static final int LOCAL_NUMBER_OF_EVALUATORS = 100; // hopefully large enough for our use....
  private static Configuration jobAndDriverConf = null;
  private static Configuration deployModeConf = null;
  private static Configuration builtJobConf = null;
  private static String serializedDAG;
  private static List<?> collectedData = new ArrayList<>();

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
    final DriverRPCServer driverRPCServer = new DriverRPCServer();
    // Registers actions for launching the DAG.
    driverRPCServer
        .registerHandler(ControlMessage.DriverToClientMessageType.DriverStarted, event -> { })
        .registerHandler(ControlMessage.DriverToClientMessageType.ResourceReady, event ->
          driverRPCServer.send(ControlMessage.ClientToDriverMessage.newBuilder()
              .setType(ControlMessage.ClientToDriverMessageType.LaunchDAG)
              .setLaunchDAG(ControlMessage.LaunchDAGMessage.newBuilder().setDag(serializedDAG).build())
              .build()))
        .registerHandler(ControlMessage.DriverToClientMessageType.DataCollected, message -> collectedData.addAll(
            SerializationUtils.deserialize(Base64.getDecoder().decode(message.getDataCollected().getData()))))
        .run();

    // Get Job and Driver Confs
    builtJobConf = getJobConf(args);
    final Configuration driverConf = getDriverConf(builtJobConf);
    final Configuration driverNcsConf = getDriverNcsConf();
    final Configuration driverMessageConfg = getDriverMessageConf();
    final Configuration executorResourceConfig = getJSONConf(builtJobConf, JobConf.ExecutorJSONPath.class,
        JobConf.ExecutorJSONContents.class);
    final Configuration bandwidthConfig = getJSONConf(builtJobConf, JobConf.BandwidthJSONPath.class,
        JobConf.BandwidthJSONContents.class);
    final Configuration clientConf = getClientConf();

    // Merge Job and Driver Confs
    jobAndDriverConf = Configurations.merge(builtJobConf, driverConf, driverNcsConf, driverMessageConfg,
        executorResourceConfig, bandwidthConfig, driverRPCServer.getListeningConfiguration());

    // Get DeployMode Conf
    deployModeConf = Configurations.merge(getDeployModeConf(builtJobConf), clientConf);

    // Launch client main
    runUserProgramMain(builtJobConf);

    driverRPCServer.shutdown();
  }

  /**
   * Launch application using the application DAG.
   *
   * @param dag the application DAG.
   */
  // When modifying the signature of this method, see CompilerTestUtil#compileDAG and make corresponding changes
  public static void launchDAG(final DAG dag) {
    try {
      if (jobAndDriverConf == null || deployModeConf == null || builtJobConf == null) {
        throw new RuntimeException("Configuration for launching driver is not ready");
      }
      serializedDAG = Base64.getEncoder().encodeToString(SerializationUtils.serialize(dag));
      // Launch and wait indefinitely for the job to finish
      final LauncherStatus launcherStatus = DriverLauncher.getLauncher(deployModeConf)
          .run(jobAndDriverConf);
      final Optional<Throwable> possibleError = launcherStatus.getError();
      if (possibleError.isPresent()) {
        throw new RuntimeException(possibleError.get());
      } else {
        LOG.info("Job successfully completed");
      }
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Run user-provided main method.
   *
   * @param jobConf the job configuration
   * @throws Exception on any exceptions on the way
   */
  private static void runUserProgramMain(final Configuration jobConf) throws Exception {
    final Injector injector = TANG.newInjector(jobConf);
    final String className = injector.getNamedInstance(JobConf.UserMainClass.class);
    final String[] args = injector.getNamedInstance(JobConf.UserMainArguments.class).split(" ");
    final Class userCode = Class.forName(className);
    final Method method = userCode.getMethod("main", String[].class);
    if (!Modifier.isStatic(method.getModifiers())) {
      throw new RuntimeException("User Main Method not static");
    }
    if (!Modifier.isPublic(userCode.getModifiers())) {
      throw new RuntimeException("User Main Class not public");
    }

    method.invoke(null, (Object) args);
  }

  /**
   * @return client configuration.
   */
  private static Configuration getClientConf() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(JobMessageHandler.class, NemoClient.JobMessageHandler.class);
    return jcb.build();
  }

  /**
   * Get driver ncs configuration.
   *
   * @return driver ncs configuration.
   * @throws InjectionException exception while injection.
   */
  private static Configuration getDriverNcsConf() throws InjectionException {
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
   * @throws InjectionException exception while injection.
   */
  private static Configuration getDriverMessageConf() throws InjectionException {
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
  private static Configuration getDriverConf(final Configuration jobConf) throws InjectionException {
    final Injector injector = TANG.newInjector(jobConf);
    final String jobId = injector.getNamedInstance(JobConf.JobId.class);
    final int driverMemory = injector.getNamedInstance(JobConf.DriverMemMb.class);
    return DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(NemoDriver.class))
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
   * @throws InjectionException exception while injection.
   */
  @VisibleForTesting
  public static Configuration getJobConf(final String[] args) throws IOException, InjectionException {
    final JavaConfigurationBuilder confBuilder = TANG.newConfigurationBuilder();
    final CommandLine cl = new CommandLine(confBuilder);
    cl.registerShortNameOfClass(JobConf.JobId.class);
    cl.registerShortNameOfClass(JobConf.UserMainClass.class);
    cl.registerShortNameOfClass(JobConf.UserMainArguments.class);
    cl.registerShortNameOfClass(JobConf.DAGDirectory.class);
    cl.registerShortNameOfClass(JobConf.OptimizationPolicy.class);
    cl.registerShortNameOfClass(JobConf.DeployMode.class);
    cl.registerShortNameOfClass(JobConf.DriverMemMb.class);
    cl.registerShortNameOfClass(JobConf.ExecutorJSONPath.class);
    cl.registerShortNameOfClass(JobConf.BandwidthJSONPath.class);
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
  private static Configuration getDeployModeConf(final Configuration jobConf) throws InjectionException {
    final Injector injector = TANG.newInjector(jobConf);
    final String deployMode = injector.getNamedInstance(JobConf.DeployMode.class);
    switch (deployMode) {
      case "local":
        return LocalRuntimeConfiguration.CONF
            .set(LocalRuntimeConfiguration.MAX_NUMBER_OF_EVALUATORS, LOCAL_NUMBER_OF_EVALUATORS)
            .build();
      case "yarn":
        return YarnClientConfiguration.CONF
            .set(YarnClientConfiguration.JVM_HEAP_SLACK, injector.getNamedInstance(JobConf.JVMHeapSlack.class))
            .build();
      default:
        throw new UnsupportedOperationException(deployMode);
    }
  }

  /**
   * Read json file and return its contents as configuration parameter.
   *
   * @param jobConf job configuration to get json path.
   * @param pathParameter named parameter represents path to the json file, or an empty string
   * @param contentsParameter named parameter represents contents of the file
   * @return configuration with contents of the file, or an empty string as value for {@code contentsParameter}
   * @throws InjectionException exception while injection.
   */
  private static Configuration getJSONConf(final Configuration jobConf,
                                           final Class<? extends Name<String>> pathParameter,
                                           final Class<? extends Name<String>> contentsParameter)
      throws InjectionException {
    final Injector injector = TANG.newInjector(jobConf);
    try {
      final String path = injector.getNamedInstance(pathParameter);
      final String contents = path.isEmpty() ? ""
          : new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
      return TANG.newConfigurationBuilder()
          .bindNamedParameter(contentsParameter, contents)
          .build();
    } catch (final IOException e) {
      throw new RuntimeException(e);
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
   * @return the collected data.
   */
  public static <T> List<T> getCollectedData() {
    return (List<T>) collectedData;
  }
}
