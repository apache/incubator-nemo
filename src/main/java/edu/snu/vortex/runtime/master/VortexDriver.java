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
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.message.MessageEnvironment;
import edu.snu.vortex.runtime.common.message.MessageSender;
import edu.snu.vortex.runtime.common.message.ncs.NcsMessageEnvironment;
import edu.snu.vortex.runtime.common.message.ncs.NcsParameters;
import edu.snu.vortex.runtime.executor.VortexContext;
import edu.snu.vortex.runtime.master.scheduler.Scheduler;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
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
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import static edu.snu.vortex.runtime.common.RuntimeAttribute.*;

/**
 * REEF Driver for Vortex.
 */
@Unit
@DriverSide
public final class VortexDriver {
  private static final Logger LOG = Logger.getLogger(VortexDriver.class.getName());

  private final EvaluatorRequestor evaluatorRequestor;
  private final NameServer nameServer;
  private final LocalAddressProvider localAddressProvider;

  // Resource configuration for single thread pool
  private final int executorNum;
  private final int executorCores;
  private final int executorMem;
  private final int executorCapacity;

  private final UserApplicationRunner userApplicationRunner;
  private final Scheduler scheduler;
  private final MessageEnvironment messageEnvironment;

  // These are hacks to get around
  // TODO #60: Specify Types in Requesting Containers
  // TODO #211: An Abstraction for Managing Resources
  private final List<ExecutorToBeLaunched> pendingEvaluators;
  private final Map<String, ExecutorToBeLaunched> executorIdToPendingContext;

  @Inject
  private VortexDriver(final EvaluatorRequestor evaluatorRequestor,
                       final Scheduler scheduler,
                       final NameServer nameServer,
                       final LocalAddressProvider localAddressProvider,
                       final UserApplicationRunner userApplicationRunner,
                       final MessageEnvironment messageEnvironment,
                       @Parameter(JobConf.ExecutorMemMb.class) final int executorMem,
                       @Parameter(JobConf.ExecutorNum.class) final int executorNum,
                       @Parameter(JobConf.ExecutorCores.class) final int executorCores,
                       @Parameter(JobConf.ExecutorCapacity.class) final int executorCapacity) {
    this.userApplicationRunner = userApplicationRunner;
    this.scheduler = scheduler;
    this.evaluatorRequestor = evaluatorRequestor;
    this.nameServer = nameServer;
    this.localAddressProvider = localAddressProvider;
    this.messageEnvironment = messageEnvironment;
    this.executorNum = executorNum;
    this.executorCores = executorCores;
    this.executorMem = executorMem;
    this.executorCapacity = executorCapacity;
    this.pendingEvaluators = new ArrayList<>();
    this.executorIdToPendingContext = new HashMap<>();
  }

  /**
   * Driver started.
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      // Launch resources
      final Set<RuntimeAttribute> completeSetOfResourceType =
          new HashSet<>(Arrays.asList(Default, Transient, Reserved, Compute, Storage));
      completeSetOfResourceType.forEach(resourceType -> {
        // For each type, request executorNum of evaluators
        // These are hacks to get around
        // TODO #60: Specify Types in Requesting Containers
        // TODO #211: An Abstraction for Managing Resources
        final ExecutorToBeLaunched executorToBeLaunched = new ExecutorToBeLaunched(resourceType, executorCapacity);
        IntStream.range(0, executorNum).forEach(i -> {
          pendingEvaluators.add(executorToBeLaunched);
        });
        evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
            .setNumber(executorNum)
            .setMemory(executorMem)
            .setNumberOfCores(executorCores)
            .build());
      });

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
      LOG.log(Level.INFO, "Container allocated");
      synchronized (this) {
        // Match one-by-one from the list assuming homogeneous evaluators
        // These are hacks to get around
        // TODO #60: Specify Types in Requesting Containers
        // TODO #211: An Abstraction for Managing Resources
        final ExecutorToBeLaunched executorToBeLaunched = pendingEvaluators.remove(0);
        final String executorId = RuntimeIdGenerator.generateExecutorId();
        executorIdToPendingContext.put(executorId, executorToBeLaunched);
        allocatedEvaluator.submitContext(getExecutorConfiguration(executorId));
      }
    }
  }

  /**
   * Context active.
   */
  public final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext activeContext) {
      LOG.log(Level.INFO, "VortexContext up and running");
      synchronized (this) {
        // These are hacks to get around
        // TODO #60: Specify Types in Requesting Containers
        // TODO #211: An Abstraction for Managing Resources
        final String executorId = activeContext.getId(); // Because we set: contextId = executorId
        final ExecutorToBeLaunched executorToBeLaunched = executorIdToPendingContext.get(executorId);

        // Connect to the executor and initiate Master side's executor representation.
        final MessageSender messageSender;
        try {
          messageSender =
              messageEnvironment.asyncConnect(executorId, MessageEnvironment.EXECUTOR_MESSAGE_RECEIVER).get();
        } catch (final Exception e) {
          throw new RuntimeException(e);
        }
        final ExecutorRepresenter executorRepresenter =
            new ExecutorRepresenter(executorId, executorToBeLaunched.getResourceType(),
                executorToBeLaunched.getExecutorCapacity(), messageSender, activeContext);

        scheduler.onExecutorAdded(executorRepresenter);
      }
    }
  }

  /**
   * Evaluator failed.
   */
  public final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      throw new RuntimeException(failedEvaluator.getEvaluatorException());
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


  private Configuration getExecutorConfiguration(final String executorId) {
    final Configuration executorConfiguration = JobConf.EXECUTOR_CONF
        .set(JobConf.EXECUTOR_ID, executorId)
        .set(JobConf.EXECUTOR_CAPACITY, executorCapacity)
        .build();

    final Configuration contextConfiguration = ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, executorId) // We set: contextId = executorId
        .set(ContextConfiguration.ON_CONTEXT_STARTED, VortexContext.ContextStartHandler.class)
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
