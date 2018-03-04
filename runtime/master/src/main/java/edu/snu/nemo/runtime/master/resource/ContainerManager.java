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
package edu.snu.nemo.runtime.master.resource;

import edu.snu.nemo.common.exception.ContainerException;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.runtime.common.message.MessageEnvironment;
import edu.snu.nemo.runtime.common.message.MessageSender;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.tang.Configuration;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * (WARNING) This class is not thread-safe.
 * Only a single thread should use the methods of this class.
 * (i.e., runtimeMasterThread in RuntimeMaster)
 *
 * Encapsulates REEF's evaluator management for containers.
 * Serves as a single point of container management in Runtime.
 * We define a unit of resource a container (an evaluator in REEF), and launch a single executor on each container.
 */
// TODO #60: Specify Types in Requesting Containers
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

  @Inject
  private ContainerManager(@Parameter(JobConf.ScheduleSerThread.class) final int scheduleSerThread,
                           final EvaluatorRequestor evaluatorRequestor,
                           final MessageEnvironment messageEnvironment) {
    this.isTerminated = false;
    this.evaluatorRequestor = evaluatorRequestor;
    this.messageEnvironment = messageEnvironment;
    this.pendingContextIdToResourceSpec = new HashMap<>();
    this.pendingContainerRequestsByContainerType = new HashMap<>();
    this.requestLatchByResourceSpecId = new HashMap<>();
    this.serializationExecutorService = Executors.newFixedThreadPool(scheduleSerThread);
  }

  /**
   * Requests containers/evaluators with the given specifications.
   * @param numToRequest number of containers to request
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
   * @param executorId of the executor to launch on this container.
   * @param allocatedContainer the allocated container.
   * @param executorConfiguration executor related configuration.
   */
  public void onContainerAllocated(final String executorId, final AllocatedEvaluator allocatedContainer,
                                   final Configuration executorConfiguration) {
    if (isTerminated) {
      LOG.info("ContainerManager is terminated, closing {}", allocatedContainer.getId());
      allocatedContainer.close();
      return;
    }

    final ResourceSpecification resourceSpecification = selectResourceSpecForContainer();
    LOG.info("Container type (" + resourceSpecification.getContainerType()
        + ") allocated, will be used for [" + executorId + "]");
    pendingContextIdToResourceSpec.put(executorId, resourceSpecification);

    allocatedContainer.submitContext(executorConfiguration);
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
    final MessageSender messageSender;
    try {
      messageSender =
          messageEnvironment.asyncConnect(executorId, MessageEnvironment.EXECUTOR_MESSAGE_LISTENER_ID).get();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }

    // Create the executor representation.
    final ExecutorRepresenter executorRepresenter =
        new ExecutorRepresenter(executorId, resourceSpec, messageSender, activeContext, serializationExecutorService,
            activeContext.getEvaluatorDescriptor().getNodeDescriptor().getName());

    LOG.info("{} is up and running at {}", executorId, executorRepresenter.getNodeName());

    requestLatchByResourceSpecId.get(resourceSpec.getResourceSpecId()).countDown();
    return Optional.of(executorRepresenter);
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
