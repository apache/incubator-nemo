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
package edu.snu.onyx.client;

import edu.snu.onyx.runtime.common.message.MessageEnvironment;
import edu.snu.onyx.runtime.common.message.ncs.NcsMessageEnvironment;
import edu.snu.onyx.runtime.common.message.ncs.NcsParameters;
import edu.snu.onyx.runtime.master.OnyxDriver;
import edu.snu.vortex.runtime.master.http.MasterHttpHandler;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.io.network.naming.LocalNameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServerConfiguration;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.*;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.IdentifierFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.reef.webserver.HttpHandlerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Job launcher.
 */
public final class JobLauncher {
  private static final Tang TANG = Tang.Factory.getTang();
  private static final Logger LOG = LoggerFactory.getLogger(JobLauncher.class.getName());
  private static final int LOCAL_NUMBER_OF_EVALUATORS = 100; // hopefully large enough for our use....

  /**
   * private constructor.
   */
  private JobLauncher() {
  }

  /**
   * Main JobLauncher method.
   * @param args arguments.
   * @throws Exception exception on the way.
   */
  public static void main(final String[] args) throws Exception {
    // Get Job and Driver Confs
    final Configuration jobConf = getJobConf(args);
    final Configuration driverConf = getDriverConf(jobConf);
    final Configuration driverNcsConf = getDriverNcsConf();
    final Configuration driverMessageConfg = getDriverMessageConf();
    final Configuration driverHttpHandlerConf = getDriverHttpConf();
    final Configuration executorResourceConfig = getExecutorResourceConf(jobConf);

    // Merge Job and Driver Confs
    final Configuration jobAndDriverConf = Configurations.merge(jobConf, driverConf, driverNcsConf, driverMessageConfg,
        driverHttpHandlerConf, executorResourceConfig);

    // Get DeployMode Conf
    final Configuration deployModeConf = getDeployModeConf(jobConf);

    // Launch and wait indefinitely for the job to finish
    final LauncherStatus launcherStatus =  DriverLauncher.getLauncher(deployModeConf).run(jobAndDriverConf);
    final Optional<Throwable> possibleError = launcherStatus.getError();
    if (possibleError.isPresent()) {
      throw new RuntimeException(possibleError.get());
    } else {
      LOG.info("Job successfully completed");
    }
  }

  private static Configuration getDriverNcsConf() throws InjectionException {
    return Configurations.merge(NameServerConfiguration.CONF.build(),
        LocalNameResolverConfiguration.CONF.build(),
        TANG.newConfigurationBuilder()
            .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
            .build());
  }

  private static Configuration getDriverMessageConf() throws InjectionException {
    return TANG.newConfigurationBuilder()
        .bindImplementation(MessageEnvironment.class, NcsMessageEnvironment.class)
        .bindNamedParameter(NcsParameters.SenderId.class, MessageEnvironment.MASTER_COMMUNICATION_ID)
        .build();
  }

  private static Configuration getDriverHttpConf() throws InjectionException {
    return HttpHandlerConfiguration.CONF
        .set(HttpHandlerConfiguration.HTTP_HANDLERS, MasterHttpHandler.class)
        .build();
  }

  private static Configuration getDriverConf(final Configuration jobConf) throws InjectionException {
    final Injector injector = TANG.newInjector(jobConf);
    final String jobId = injector.getNamedInstance(JobConf.JobId.class);
    final int driverMemory = injector.getNamedInstance(JobConf.DriverMemMb.class);
    return DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(OnyxDriver.class))
        .set(DriverConfiguration.ON_DRIVER_STARTED, OnyxDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, OnyxDriver.AllocatedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, OnyxDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_FAILED, OnyxDriver.FailedEvaluatorHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_FAILED, OnyxDriver.FailedContextHandler.class)
        .set(DriverConfiguration.ON_DRIVER_STOP, OnyxDriver.DriverStopHandler.class)
        .set(DriverConfiguration.DRIVER_IDENTIFIER, jobId)
        .set(DriverConfiguration.DRIVER_MEMORY, driverMemory)
        .build();
  }

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
    cl.registerShortNameOfClass(JobConf.ExecutorJsonPath.class);
    cl.registerShortNameOfClass(JobConf.JVMHeapSlack.class);
    cl.registerShortNameOfClass(JobConf.IORequestHandleThreadsTotal.class);
    cl.registerShortNameOfClass(JobConf.SchedulerTimeoutMs.class);
    cl.registerShortNameOfClass(JobConf.MaxScheduleAttempt.class);
    cl.registerShortNameOfClass(JobConf.PartitionTransferInboundNumThreads.class);
    cl.registerShortNameOfClass(JobConf.PartitionTransferOutboundNumThreads.class);
    cl.registerShortNameOfClass(JobConf.PartitionTransferOutboundBufferSize.class);
    cl.registerShortNameOfClass(JobConf.PartitionTransportServerPort.class);
    cl.registerShortNameOfClass(JobConf.PartitionTransportServerBacklog.class);
    cl.registerShortNameOfClass(JobConf.PartitionTransportServerNumListeningThreads.class);
    cl.registerShortNameOfClass(JobConf.PartitionTransportServerNumWorkingThreads.class);
    cl.registerShortNameOfClass(JobConf.PartitionTransportClientNumThreads.class);
    cl.processCommandLine(args);
    return confBuilder.build();
  }

  public static Configuration getDeployModeConf(final Configuration jobConf) throws InjectionException {
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

  public static Configuration getExecutorResourceConf(final Configuration jobConf) throws InjectionException {
    final Injector injector = TANG.newInjector(jobConf);
    try {
      final String path = injector.getNamedInstance(JobConf.ExecutorJsonPath.class);
      final String contents = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
      return TANG.newConfigurationBuilder()
          .bindNamedParameter(JobConf.ExecutorJsonContents.class, contents)
          .build();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
