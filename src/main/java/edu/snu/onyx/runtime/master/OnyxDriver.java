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
package edu.snu.onyx.runtime.master;

import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.snu.onyx.client.JobConf;
import edu.snu.onyx.runtime.common.RuntimeIdGenerator;
import edu.snu.onyx.runtime.common.message.MessageEnvironment;
import edu.snu.onyx.runtime.common.message.ncs.NcsMessageEnvironment;
import edu.snu.onyx.runtime.common.message.ncs.NcsParameters;
import edu.snu.onyx.runtime.executor.OnyxContext;
import edu.snu.onyx.runtime.master.resource.ContainerManager;
import edu.snu.onyx.runtime.master.resource.ResourceSpecification;
import edu.snu.onyx.runtime.master.scheduler.Scheduler;
import org.apache.reef.annotations.audience.DriverSide;
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

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REEF Driver for Onyx.
 */
@Unit
@DriverSide
public final class OnyxDriver {
  private static final Logger LOG = LoggerFactory.getLogger(OnyxDriver.class.getName());

  private final NameServer nameServer;
  private final LocalAddressProvider localAddressProvider;

  private final String resourceSpecificationString;

  private final UserApplicationRunner userApplicationRunner;
  private final ContainerManager containerManager;
  private final Scheduler scheduler;
  private final String jobId;
  private final String localDirectory;
  private final String glusterDirectory;

  @Inject
  private OnyxDriver(final ContainerManager containerManager,
                     final Scheduler scheduler,
                     final UserApplicationRunner userApplicationRunner,
                     final NameServer nameServer,
                     final LocalAddressProvider localAddressProvider,
                     @Parameter(JobConf.ExecutorJsonContents.class) final String resourceSpecificationString,
                     @Parameter(JobConf.JobId.class) final String jobId,
                     @Parameter(JobConf.FileDirectory.class) final String localDirectory,
                     @Parameter(JobConf.GlusterVolumeDirectory.class) final String glusterDirectory) {
    this.userApplicationRunner = userApplicationRunner;
    this.containerManager = containerManager;
    this.scheduler = scheduler;
    this.nameServer = nameServer;
    this.localAddressProvider = localAddressProvider;
    this.resourceSpecificationString = resourceSpecificationString;
    this.jobId = jobId;
    this.localDirectory = localDirectory;
    this.glusterDirectory = glusterDirectory;
  }

  /**
   * Driver started.
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      try {
        final ObjectMapper objectMapper = new ObjectMapper();
        final TreeNode jsonRootNode = objectMapper.readTree(resourceSpecificationString);

        for (int i = 0; i < jsonRootNode.size(); i++) {
          final TreeNode resourceNode = jsonRootNode.get(i);
          final ResourceSpecification.Builder builder = ResourceSpecification.newBuilder();
          builder.setContainerType(resourceNode.get("type").traverse().nextTextValue());
          builder.setMemory(resourceNode.get("memory_mb").traverse().getIntValue());
          builder.setCapacity(4);
          final int executorNum = resourceNode.path("num").traverse().nextIntValue(1);
          containerManager.requestContainer(executorNum, builder.build());
        }
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }

      // Launch user application (with a new thread)
      final ExecutorService userApplicationRunnerThread = Executors.newSingleThreadExecutor();
      userApplicationRunnerThread.execute(userApplicationRunner);
      userApplicationRunnerThread.shutdown();
    }
  }

  /**
   * Container allocated.
   */
  public final class AllocatedEvaluatorHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final String executorId = RuntimeIdGenerator.generateExecutorId();
      containerManager.onContainerAllocated(executorId, allocatedEvaluator,
          getExecutorConfiguration(executorId, 4));
    }
  }

  /**
   * Context active.
   */
  public final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext activeContext) {
      containerManager.onExecutorLaunched(activeContext);
      scheduler.onExecutorAdded(activeContext.getId());
      LOG.info("Exit: ActiveContextHandler");
    }
  }

  /**
   * Evaluator failed.
   */
  public final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      LOG.info("failed evaluator: " + failedEvaluator.getId());
      LOG.info("failed evaluator context list: " + failedEvaluator.getFailedContextList());
      final List<FailedContext> failedContexts = failedEvaluator.getFailedContextList();

      if (failedContexts.isEmpty()) {
        containerManager.onContainerRemoved(failedEvaluator.getId());
      } else {
        // The list size is 0 if the evaluator failed before an executor started. For now, the size is 1 otherwise.
        failedContexts.forEach(failedContext -> {
          final String failedExecutorId = failedContext.getId();
          containerManager.onExecutorRemoved(failedExecutorId);
          scheduler.onExecutorRemoved(failedExecutorId);
        });
      }
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
    }
  }

  private Configuration getExecutorConfiguration(final String executorId, final int executorCapacity) {
    final Configuration executorConfiguration = JobConf.EXECUTOR_CONF
        .set(JobConf.EXECUTOR_ID, executorId)
        .set(JobConf.EXECUTOR_CAPACITY, executorCapacity)
        .set(JobConf.GLUSTER_DISK_DIRECTORY, glusterDirectory)
        .set(JobConf.LOCAL_DISK_DIRECTORY, localDirectory)
        .set(JobConf.JOB_ID, jobId)
        .build();

    final Configuration contextConfiguration = ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, executorId) // We set: contextId = executorId
        .set(ContextConfiguration.ON_CONTEXT_STARTED, OnyxContext.ContextStartHandler.class)
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
        .bindImplementation(MessageEnvironment.class, NcsMessageEnvironment.class)
        .bindNamedParameter(NcsParameters.SenderId.class, executorId)
        .build();
  }
}
