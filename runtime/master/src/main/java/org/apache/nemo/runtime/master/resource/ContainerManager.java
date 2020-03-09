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
package org.apache.nemo.runtime.master.resource;

import org.apache.nemo.common.exception.ContainerException;
import org.apache.nemo.common.ir.executionproperty.ResourceSpecification;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.common.message.FailedMessageSender;
import org.apache.nemo.runtime.common.message.MessageEnvironment;
import org.apache.nemo.runtime.common.message.MessageSender;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * (WARNING) This class is not thread-safe.
 * Only a single thread should use the methods of this class.
 * (i.e., runtimeMasterThread in RuntimeMaster)
 * <p>
 * Encapsulates REEF's evaluator management for containers.
 * Serves as a single point of container management in Runtime.
 * We define a unit of resource a container (an evaluator in REEF), and launch a single executor on each container.
 */
// We need an overall cleanup of this class after #60 is resolved.
@DriverSide
@NotThreadSafe
public final class ContainerManager {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerManager.class.getName());

  private boolean isTerminated;

  private final EvaluatorRequestor evaluatorRequestor;
  private final MessageEnvironment messageEnvironment;
  private final ExecutorService serializationExecutorService; // Executor service for scheduling message serialization.

  /**
   * A map containing a latch for the container requests for each resource spec ID.
   */
  private final Map<String, CountDownLatch> requestLatchByResourceSpecId;

  /**
   * Keeps track of evaluator and context requests.
   */
  private final Map<String, ResourceSpecification> pendingContextIdToResourceSpec;
  private final Map<String, List<ResourceSpecification>> pendingContainerRequestsByContainerType;

  /**
   * Remember the resource spec for each evaluator.
   */
  private final Map<String, ResourceSpecification> evaluatorIdToResourceSpec;

  @Inject
  private ContainerManager(@Parameter(JobConf.ScheduleSerThread.class) final int scheduleSerThread,
                           final EvaluatorRequestor evaluatorRequestor,
                           final MessageEnvironment messageEnvironment) {
    this.isTerminated = false;
    this.evaluatorRequestor = evaluatorRequestor;
    this.messageEnvironment = messageEnvironment;
    this.pendingContextIdToResourceSpec = new HashMap<>();
    this.pendingContainerRequestsByContainerType = new HashMap<>();
    this.evaluatorIdToResourceSpec = new HashMap<>();
    this.requestLatchByResourceSpecId = new HashMap<>();
    this.serializationExecutorService = Executors.newFixedThreadPool(scheduleSerThread);
  }

  /**
   * Requests containers/evaluators with the given specifications.
   *
   * @param numToRequest          number of containers to request
   * @param resourceSpecification containing the specifications of
   */
  public void requestContainer(final int numToRequest, final ResourceSpecification resourceSpecification) {
    if (isTerminated) {
      LOG.info("ContainerManager is terminated, ignoring {}", resourceSpecification.toString());
      return;
    }

    if (numToRequest > 0) {
      // Create a list of executor specifications to be used when containers are allocated.
      final List<ResourceSpecification> resourceSpecificationList = new ArrayList<>(numToRequest);
      for (int i = 0; i < numToRequest; i++) {
        resourceSpecificationList.add(resourceSpecification);
      }

      // Mark the request as pending with the given specifications.
      pendingContainerRequestsByContainerType.putIfAbsent(resourceSpecification.getContainerType(), new ArrayList<>());
      pendingContainerRequestsByContainerType.get(resourceSpecification.getContainerType())
        .addAll(resourceSpecificationList);

      requestLatchByResourceSpecId.put(resourceSpecification.getResourceSpecId(),
        new CountDownLatch(numToRequest));

      // Request the evaluators
      evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
        .setNumber(numToRequest)
        .setMemory(resourceSpecification.getMemory())
        .setNumberOfCores(resourceSpecification.getCapacity())
        .build());
    } else {
      LOG.info("Request {} containers", numToRequest);
    }
  }

  /**
   * Take the necessary actions in container manager once a container a is allocated.
   *
   * @param executorId            of the executor to launch on this container.
   * @param allocatedContainer    the allocated container.
   * @param executorConfiguration executor related configuration.
   */
  public void onContainerAllocated(final String executorId,
                                   final AllocatedEvaluator allocatedContainer,
                                   final Configuration executorConfiguration) {
    if (isTerminated) {
      LOG.info("ContainerManager is terminated, closing {}", allocatedContainer.getId());
      allocatedContainer.close();
      return;
    }

    final ResourceSpecification resourceSpecification = selectResourceSpecForContainer();
    final List<Configuration> configurationsToMerge = new ArrayList<>();

    evaluatorIdToResourceSpec.put(allocatedContainer.getId(), resourceSpecification);

    LOG.info("Container type (" + resourceSpecification.getContainerType()
      + ") allocated, will be used for [" + executorId + "]");
    pendingContextIdToResourceSpec.put(executorId, resourceSpecification);

    configurationsToMerge.add(executorConfiguration);

    // ExecutorMemory handling
    configurationsToMerge.add(Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(JobConf.ExecutorMemoryMb.class, String.valueOf(resourceSpecification.getMemory()))
        .build()
    );

    // MaxOffheapRatio handling
    resourceSpecification.getMaxOffheapRatio().ifPresent(value ->
      configurationsToMerge.add(Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(JobConf.MaxOffheapRatio.class, String.valueOf(value))
        .build()
      )
    );

    // Poison handling
    resourceSpecification.getPoisonSec().ifPresent(value ->
      configurationsToMerge.add(Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(JobConf.ExecutorPoisonSec.class, String.valueOf(value))
        .build()
      )
    );

    allocatedContainer.submitContext(Configurations.merge(configurationsToMerge));
  }

  /**
   * Initializes master's connection to the container once launched.
   * A representation of the executor to reside in master is created.
   *
   * @param activeContext for the launched container.
   * @return a representation of the executor. (return an empty Optional if terminated)
   */
  public Optional<ExecutorRepresenter> onContainerLaunched(final ActiveContext activeContext) {
    if (isTerminated) {
      LOG.info("ContainerManager is terminated, closing {}", activeContext.getId());
      activeContext.close();
      return Optional.empty();
    }

    // We set contextId = executorId in NemoDriver when we generate executor configuration.
    final String executorId = activeContext.getId();
    final ResourceSpecification resourceSpec = pendingContextIdToResourceSpec.remove(executorId);

    // Connect to the executor and initiate Master side's executor representation.
    MessageSender messageSender;
    try {
      messageSender =
        messageEnvironment.asyncConnect(executorId, MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID).get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      messageSender = new FailedMessageSender();
    } catch (final ExecutionException e) {
      // TODO #140: Properly classify and handle each RPC failure
      messageSender = new FailedMessageSender();
    }

    // Create the executor representation.
    final ExecutorRepresenter executorRepresenter =
      new DefaultExecutorRepresenter(executorId, resourceSpec, messageSender,
        activeContext, serializationExecutorService,
        activeContext.getEvaluatorDescriptor().getNodeDescriptor().getName());

    requestLatchByResourceSpecId.get(resourceSpec.getResourceSpecId()).countDown();

    return Optional.of(executorRepresenter);
  }

  /**
   * Re-acquire a new container using the failed container's resource spec.
   *
   * @param failedEvaluatorId of the failed evaluator
   * @return the resource specification of the failed evaluator
   */
  public ResourceSpecification onContainerFailed(final String failedEvaluatorId) {
    final ResourceSpecification resourceSpecification = evaluatorIdToResourceSpec.remove(failedEvaluatorId);
    if (resourceSpecification == null) {
      throw new IllegalStateException(failedEvaluatorId + " not in " + evaluatorIdToResourceSpec);
    }
    requestContainer(1, resourceSpecification);
    return resourceSpecification;
  }

  public void terminate() {
    if (isTerminated) {
      throw new IllegalStateException("Cannot terminate twice");
    }
    isTerminated = true;
  }

  /**
   * Selects an executor specification for the executor to be launched on a container.
   * Important! This is a "hack" to get around the inability to mark evaluators with Node Labels in REEF.
   *
   * @return the selected executor specification.
   */
  private ResourceSpecification selectResourceSpecForContainer() {
    ResourceSpecification selectedResourceSpec = null;
    for (final Map.Entry<String, List<ResourceSpecification>> entry
      : pendingContainerRequestsByContainerType.entrySet()) {
      if (entry.getValue().size() > 0) {
        selectedResourceSpec = entry.getValue().remove(0);
        break;
      }
    }

    if (selectedResourceSpec != null) {
      return selectedResourceSpec;
    }
    throw new ContainerException(new Throwable("We never requested for an extra container"));
  }
}
