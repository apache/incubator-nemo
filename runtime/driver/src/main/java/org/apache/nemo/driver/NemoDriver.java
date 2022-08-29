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
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.nemo.common.ir.IdManager;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.ResourceSitePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.XGBoostPass;
import org.apache.nemo.conf.DataPlaneConf;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.runtime.common.message.ClientRPC;
import org.apache.nemo.runtime.common.message.MessageParameters;
import org.apache.nemo.runtime.master.BroadcastManagerMaster;
import org.apache.nemo.runtime.master.RuntimeMaster;
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
import java.io.Serializable;
import java.util.Map;
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
  private final Integer streamMetricPeriod;
  private final Integer latencyMarkPeriod;
  private final String localDirectory;
  private final String glusterDirectory;
  private final ClientRPC clientRPC;
  private final DataPlaneConf dataPlaneConf;

  private static ExecutorService runnerThread = Executors.newSingleThreadExecutor(
    new BasicThreadFactory.Builder().namingPattern("User App thread-%d").build());

  // Client for sending log messages
  private final RemoteClientMessageLoggingHandler handler;

  @Inject
  private NemoDriver(final UserApplicationRunner userApplicationRunner,
                     final RuntimeMaster runtimeMaster,
                     final NameServer nameServer,
                     final LocalAddressProvider localAddressProvider,
                     final JobMessageObserver client,
                     final ClientRPC clientRPC,
                     final DataPlaneConf dataPlaneConf,
                     @Parameter(JobConf.ExecutorJSONContents.class) final String resourceSpecificationString,
                     @Parameter(JobConf.BandwidthJSONContents.class) final String bandwidthString,
                     @Parameter(JobConf.JobId.class) final String jobId,
                     @Parameter(JobConf.StreamMetricPeriod.class) final int streamMetricPeriod,
                     @Parameter(JobConf.LatencyMarkPeriod.class) final int latencyMarkPeriod,
                     @Parameter(JobConf.FileDirectory.class) final String localDirectory,
                     @Parameter(JobConf.GlusterVolumeDirectory.class) final String glusterDirectory) {
    IdManager.setInDriver();
    this.userApplicationRunner = userApplicationRunner;
    this.runtimeMaster = runtimeMaster;
    this.nameServer = nameServer;
    this.localAddressProvider = localAddressProvider;
    this.resourceSpecificationString = resourceSpecificationString;
    this.jobId = jobId;
    this.streamMetricPeriod = streamMetricPeriod;
    this.latencyMarkPeriod = latencyMarkPeriod;
    this.localDirectory = localDirectory;
    this.glusterDirectory = glusterDirectory;
    this.handler = new RemoteClientMessageLoggingHandler(client);
    this.clientRPC = clientRPC;
    this.dataPlaneConf = dataPlaneConf;
    // TODO #69: Support job-wide execution property
    ResourceSitePass.setBandwidthSpecificationString(bandwidthString);
    clientRPC.registerHandler(ControlMessage.ClientToDriverMessageType.Notification, this::handleNotification);
    clientRPC.registerHandler(ControlMessage.ClientToDriverMessageType.LaunchDAG, message -> {
      startSchedulingUserDAG(message.getLaunchDAG().getDag());
      final Map<Serializable, Object> broadcastVars =
        SerializationUtils.deserialize(message.getLaunchDAG().getBroadcastVars().toByteArray());
      BroadcastManagerMaster.registerBroadcastVariablesFromClient(broadcastVars);
    });
    clientRPC.registerHandler(ControlMessage.ClientToDriverMessageType.DriverShutdown, message -> shutdown());
    // Send DriverStarted message to the client
    clientRPC.send(ControlMessage.DriverToClientMessage.newBuilder()
      .setType(ControlMessage.DriverToClientMessageType.DriverStarted).build());
  }

  /**
   * Setup the logger that forwards logging messages to the client.
   */
  private void setUpLogger() {
    final java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
    rootLogger.addHandler(handler);
  }

  /**
   * Trigger shutdown of the driver and the runtime master.
   */
  private void shutdown() {
    LOG.info("Driver shutdown initiated");
    runnerThread.execute(runtimeMaster::terminate);
    runnerThread.shutdown();
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
      final String executorId = RuntimeIdManager.generateExecutorId();
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
        clientRPC.send(ControlMessage.DriverToClientMessage.newBuilder()
          .setType(ControlMessage.DriverToClientMessageType.DriverReady).build());
      }
    }
  }

  /**
   * Start to schedule a submitted user DAG.
   *
   * @param dagString the serialized DAG to schedule.
   */
  private void startSchedulingUserDAG(final String dagString) {
    runnerThread.execute(() -> {
      userApplicationRunner.run(dagString);
      // send driver notification that user application is done.
      clientRPC.send(ControlMessage.DriverToClientMessage.newBuilder()
        .setType(ControlMessage.DriverToClientMessageType.ExecutionDone).build());
      // flush metrics
      runtimeMaster.flushMetrics();
    });
  }

  /**
   * handler for notifications from the client.
   *
   * @param message message from the client.
   */
  private void handleNotification(final ControlMessage.ClientToDriverMessage message) {
    switch (message.getMessage().getOptimizationType()) {
      case XGBoost:
        XGBoostPass.pushMessage(message.getMessage().getData());
        break;
      default:
        break;
    }
  }

  /**
   * Evaluator failed.
   */
  public final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      runtimeMaster.onExecutorFailed(failedEvaluator);
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
      clientRPC.shutdown();
    }
  }

  private Configuration getExecutorConfiguration(final String executorId) {
    final Configuration executorConfiguration = JobConf.EXECUTOR_CONF
      .set(JobConf.EXECUTOR_ID, executorId)
      .set(JobConf.STREAM_METRIC_PERIOD, streamMetricPeriod)
      .set(JobConf.LATENCYMARK_PERIOD, latencyMarkPeriod)
      .set(JobConf.GLUSTER_DISK_DIRECTORY, glusterDirectory)
      .set(JobConf.LOCAL_DISK_DIRECTORY, localDirectory)
      .set(JobConf.JOB_ID, jobId)
      .build();

    final Configuration contextConfiguration = ContextConfiguration.CONF
      .set(ContextConfiguration.IDENTIFIER, executorId) // We set: contextId = executorId
      .set(ContextConfiguration.ON_CONTEXT_STARTED, NemoContext.ContextStartHandler.class)
      .set(ContextConfiguration.ON_CONTEXT_STOP, NemoContext.ContextStopHandler.class)
      .build();

    final Configuration ncsConfiguration = getExecutorNcsConfiguration();
    final Configuration messageConfiguration = getExecutorMessageConfiguration(executorId);
    final Configuration dataPlaneConfiguration = dataPlaneConf.getDataPlaneConfiguration();

    return Configurations.merge(executorConfiguration, contextConfiguration, ncsConfiguration,
      messageConfiguration, dataPlaneConfiguration);
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
