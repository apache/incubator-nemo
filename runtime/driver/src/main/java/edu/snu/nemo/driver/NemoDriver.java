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

import edu.snu.nemo.common.ir.IdManager;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.message.MessageParameters;
import edu.snu.nemo.runtime.master.RuntimeMaster;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.client.JobMessageObserver;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerAddr;
import org.apache.reef.io.network.naming.parameters.NameResolverNameServerPort;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.LogManager;

/**
 * REEF Driver for Nemo.
 */
@Unit
@DriverSide
public final class NemoDriver {
  private static final Logger LOG = LoggerFactory.getLogger(NemoDriver.class.getName());

  private final NameServer nameServer;
  private final LocalAddressProvider localAddressProvider;

  private final String resourceSpecificationString;

  private final UserApplicationRunner userApplicationRunner;
  private final RuntimeMaster runtimeMaster;
  private final String jobId;
  private final String localDirectory;
  private final String glusterDirectory;

  // Client for sending log messages
  private final JobMessageObserver client;
  private final RemoteClientMessageLoggingHandler handler;

  @Inject
  private NemoDriver(final UserApplicationRunner userApplicationRunner,
                     final RuntimeMaster runtimeMaster,
                     final NameServer nameServer,
                     final LocalAddressProvider localAddressProvider,
                     final JobMessageObserver client,
                     @Parameter(JobConf.ExecutorJsonContents.class) final String resourceSpecificationString,
                     @Parameter(JobConf.JobId.class) final String jobId,
                     @Parameter(JobConf.FileDirectory.class) final String localDirectory,
                     @Parameter(JobConf.GlusterVolumeDirectory.class) final String glusterDirectory) {
    IdManager.setInDriver();
    this.userApplicationRunner = userApplicationRunner;
    this.runtimeMaster = runtimeMaster;
    this.nameServer = nameServer;
    this.localAddressProvider = localAddressProvider;
    this.resourceSpecificationString = resourceSpecificationString;
    this.jobId = jobId;
    this.localDirectory = localDirectory;
    this.glusterDirectory = glusterDirectory;
    this.client = client;
    this.handler = new RemoteClientMessageLoggingHandler(client);
  }

  /**
   * Setup the logger that forwards logging messages to the client.
   */
  private void setUpLogger() {
    final java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
    rootLogger.addHandler(handler);
  }

  /**
   * Driver started.
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      setUpLogger();
      runtimeMaster.requestContainer(resourceSpecificationString);
    }
  }

  /**
   * Container allocated.
   */
  public final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final String executorId = RuntimeIdGenerator.generateExecutorId();
      runtimeMaster.onContainerAllocated(executorId, allocatedEvaluator,
          getExecutorConfiguration(executorId));
    }
  }

  /**
   * Context active.
   */
  public final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext activeContext) {
      final boolean finalExecutorLaunched = runtimeMaster.onExecutorLaunched(activeContext);

      if (finalExecutorLaunched) {
        startSchedulingUserApplication();
      }
    }
  }

  private void startSchedulingUserApplication() {
    // Launch user application (with a new thread)
    final ExecutorService userApplicationRunnerThread = Executors.newSingleThreadExecutor();
    userApplicationRunnerThread.execute(userApplicationRunner);
    userApplicationRunnerThread.shutdown();
  }

  /**
   * Evaluator failed.
   */
  public final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      // The list size is 0 if the evaluator failed before an executor started. For now, the size is 1 otherwise.
      failedEvaluator.getFailedContextList().forEach(failedContext -> {
        final String failedExecutorId = failedContext.getId();
        runtimeMaster.onExecutorFailed(failedExecutorId);
      });
      throw new RuntimeException(failedEvaluator.getId()
          + " failed. See driver's log for the stack trace in executor.");
    }
  }

  /**
   * Context failed.
   */
  public final class FailedContextHandler implements EventHandler<FailedContext> {
    @Override
    public void onNext(final FailedContext failedContext) {
      throw new RuntimeException(failedContext.getId() + " failed. See driver's log for the stack trace in executor.",
          failedContext.asError());
    }
  }

  /**
   * Driver stopped.
   */
  public final class DriverStopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime stopTime) {
      handler.close();
    }
  }

  private Configuration getExecutorConfiguration(final String executorId) {
    final Configuration executorConfiguration = JobConf.EXECUTOR_CONF
        .set(JobConf.EXECUTOR_ID, executorId)
        .set(JobConf.GLUSTER_DISK_DIRECTORY, glusterDirectory)
        .set(JobConf.LOCAL_DISK_DIRECTORY, localDirectory)
        .set(JobConf.JOB_ID, jobId)
        .build();

    final Configuration contextConfiguration = ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, executorId) // We set: contextId = executorId
        .set(ContextConfiguration.ON_CONTEXT_STARTED, NemoContext.ContextStartHandler.class)
        .set(ContextConfiguration.ON_CONTEXT_STOP, NemoContext.ContextStopHandler.class)
        .build();

    final Configuration ncsConfiguration =  getExecutorNcsConfiguration();
    final Configuration messageConfiguration = getExecutorMessageConfiguration(executorId);

    return Configurations.merge(executorConfiguration, contextConfiguration, ncsConfiguration, messageConfiguration);
  }

  private Configuration getExecutorNcsConfiguration() {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(NameResolverNameServerPort.class, Integer.toString(nameServer.getPort()))
        .bindNamedParameter(NameResolverNameServerAddr.class, localAddressProvider.getLocalAddress())
        .bindImplementation(IdentifierFactory.class, StringIdentifierFactory.class)
        .build();
  }

  private Configuration getExecutorMessageConfiguration(final String executorId) {
    return Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(MessageParameters.SenderId.class, executorId)
        .build();
  }
}
