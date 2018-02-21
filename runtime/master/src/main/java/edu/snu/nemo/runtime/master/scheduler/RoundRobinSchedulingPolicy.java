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
package edu.snu.nemo.runtime.master.scheduler;

import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.common.exception.SchedulingException;
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.nemo.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.nemo.runtime.common.state.TaskGroupState;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.master.resource.ExecutorRegistry;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.stream.Collectors;

/**
 * {@inheritDoc}
 * A Round-Robin implementation used by {@link BatchSingleJobScheduler}.
 *
 * This policy keeps a list of available {@link ExecutorRepresenter} for each type of container.
 * The RR policy is used for each container type when trying to schedule a task group.
 */
@ThreadSafe
@DriverSide
public final class RoundRobinSchedulingPolicy implements SchedulingPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(RoundRobinSchedulingPolicy.class.getName());

  private final int scheduleTimeoutMs;

  private final ExecutorRegistry executorRegistry;

  /**
   * Thread safety is provided by this lock as multiple threads can call the methods in this class concurrently.
   */
  private final Lock lock;

  /**
   * Executor allocation is achieved by putting conditions for each container type.
   * The condition blocks when there is no executor of the container type available,
   * and is released when such an executor becomes available (either by an extra executor, or a task group completion).
   */
  private final Map<String, Condition> conditionByContainerType;

  /**
   * The pool of executors available for each container type.
   */
  private final Map<String, List<String>> executorIdByContainerType;

  /**
   * The index of the next executor to be assigned for each container type.
   * This map allows the executor index computation of the RR scheduling.
   */
  private final Map<String, Integer> nextExecutorIndexByContainerType;

  @Inject
  public RoundRobinSchedulingPolicy(final ExecutorRegistry executorRegistry,
                                    @Parameter(JobConf.SchedulerTimeoutMs.class) final int scheduleTimeoutMs) {
    this.scheduleTimeoutMs = scheduleTimeoutMs;
    this.executorRegistry = executorRegistry;
    this.lock = new ReentrantLock();
    this.executorIdByContainerType = new HashMap<>();
    this.conditionByContainerType = new HashMap<>();
    this.nextExecutorIndexByContainerType = new HashMap<>();
    initializeContainerTypeIfAbsent(ExecutorPlacementProperty.NONE); // Need this to avoid potential null errors
  }

  public long getScheduleTimeoutMs() {
    return scheduleTimeoutMs;
  }

  @Override
  public boolean scheduleTaskGroup(final ScheduledTaskGroup scheduledTaskGroup,
                                   final JobStateManager jobStateManager) {
    lock.lock();
    try {
      final String containerType = scheduledTaskGroup.getContainerType();
      initializeContainerTypeIfAbsent(containerType);

      Optional<String> executorId = selectExecutorByRR(containerType);
      if (!executorId.isPresent()) { // If there is no available executor to schedule this task group now,
        // TODO #696 Sleep Time Per Container Type in Scheduling Policy
        final boolean executorAvailable =
            conditionByContainerType.get(containerType).await(scheduleTimeoutMs, TimeUnit.MILLISECONDS);
        if (executorAvailable) { // if an executor has become available before scheduleTimeoutMs,
          executorId = selectExecutorByRR(containerType);
          if (executorId.isPresent()) {
            scheduleTaskGroup(selectExecutorByRR(containerType).get(), scheduledTaskGroup, jobStateManager);
            return true;
          } else {
            throw new SchedulingException(new Throwable("An executor must be available at this point"));
          }
        }
        return false;
      } else {
        scheduleTaskGroup(executorId.get(), scheduledTaskGroup, jobStateManager);
        return true;
      }
    } catch (final Exception e) {
      throw new SchedulingException(e);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Sticks to the RR policy to select an executor for the next task group.
   * It checks the task groups running (as compared to each executor's capacity).
   *
   * @param containerType to select an executor for.
   * @return (optionally) the selected executor.
   */
  private Optional<String> selectExecutorByRR(final String containerType) {
    final List<String> candidateExecutorIds = (containerType.equals(ExecutorPlacementProperty.NONE))
        ? getAllContainers() // all containers
        : executorIdByContainerType.get(containerType); // containers of a particular type

    if (candidateExecutorIds != null && !candidateExecutorIds.isEmpty()) {
      final int numExecutors = candidateExecutorIds.size();
      int nextExecutorIndex = nextExecutorIndexByContainerType.get(containerType);
      for (int i = 0; i < numExecutors; i++) {
        final int index = (nextExecutorIndex + i) % numExecutors;
        final String selectedExecutorId = candidateExecutorIds.get(index);

        final ExecutorRepresenter executor = executorRegistry.getRunningExecutorRepresenter(selectedExecutorId);
        if (hasFreeSlot(executor)) {
          nextExecutorIndex = (index + 1) % numExecutors;
          nextExecutorIndexByContainerType.put(containerType, nextExecutorIndex);
          return Optional.of(selectedExecutorId);
        }
      }
    }

    return Optional.empty();
  }

  /**
   * Schedules and sends a TaskGroup to the given executor.
   *
   * @param executorId         of the executor to execute the TaskGroup.
   * @param scheduledTaskGroup to assign.
   * @param jobStateManager    which the TaskGroup belongs to.
   */
  private void scheduleTaskGroup(final String executorId,
                                 final ScheduledTaskGroup scheduledTaskGroup,
                                 final JobStateManager jobStateManager) {
    jobStateManager.onTaskGroupStateChanged(scheduledTaskGroup.getTaskGroupId(), TaskGroupState.State.EXECUTING);

    final ExecutorRepresenter executor = executorRegistry.getRunningExecutorRepresenter(executorId);
    LOG.info("Scheduling {} to {}",
        new Object[]{scheduledTaskGroup.getTaskGroupId(), executorId});
    executor.onTaskGroupScheduled(scheduledTaskGroup);
  }

  private List<String> getAllContainers() {
    return executorIdByContainerType.values().stream()
        .flatMap(List::stream) // flatten the list of lists to a flat stream
        .collect(Collectors.toList()); // convert the stream to a list
  }

  private boolean hasFreeSlot(final ExecutorRepresenter executor) {
    LOG.debug("Has Free Slot: " + executor.getExecutorId());
    LOG.debug("Running TaskGroups: " + executor.getRunningTaskGroups());
    return executor.getRunningTaskGroups().size() < executor.getExecutorCapacity();
  }

  private void initializeContainerTypeIfAbsent(final String containerType) {
    executorIdByContainerType.putIfAbsent(containerType, new ArrayList<>());
    nextExecutorIndexByContainerType.putIfAbsent(containerType, 0);
    conditionByContainerType.putIfAbsent(containerType, lock.newCondition());
  }

  private void signalPossiblyWaitingScheduler(final String typeOfContainerWithNewFreeSlot) {
    conditionByContainerType.get(typeOfContainerWithNewFreeSlot).signal();
    if (!typeOfContainerWithNewFreeSlot.equals(ExecutorPlacementProperty.NONE)) {
      conditionByContainerType.get(ExecutorPlacementProperty.NONE).signal();
    }
  }

  @Override
  public void onExecutorAdded(final String executorId) {
    lock.lock();
    try {
      final ExecutorRepresenter executor = executorRegistry.getRunningExecutorRepresenter(executorId);
      final String containerType = executor.getContainerType();
      initializeContainerTypeIfAbsent(containerType);

      executorIdByContainerType.get(containerType)
          .add(nextExecutorIndexByContainerType.get(containerType), executorId);
      signalPossiblyWaitingScheduler(containerType);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Set<String> onExecutorRemoved(final String executorId) {
    lock.lock();
    try {
      final ExecutorRepresenter executor = executorRegistry.getFailedExecutorRepresenter(executorId);
      final String containerType = executor.getContainerType();

      final List<String> executorIdList = executorIdByContainerType.get(containerType);
      int nextExecutorIndex = nextExecutorIndexByContainerType.get(containerType);

      final int executorAssignmentLocation = executorIdList.indexOf(executorId);
      if (executorAssignmentLocation < nextExecutorIndex) {
        nextExecutorIndexByContainerType.put(containerType, nextExecutorIndex - 1);
      } else if (executorAssignmentLocation == nextExecutorIndex) {
        nextExecutorIndexByContainerType.put(containerType, 0);
      }
      executorIdList.remove(executorId);

      return Collections.unmodifiableSet(executor.getRunningTaskGroups());
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void onTaskGroupExecutionComplete(final String executorId, final String taskGroupId) {
    lock.lock();
    try {
      final ExecutorRepresenter executor = executorRegistry.getRunningExecutorRepresenter(executorId);
      executor.onTaskGroupExecutionComplete(taskGroupId);
      LOG.info("{" + taskGroupId + "} completed in [" + executorId + "]");

      // the scheduler thread may be waiting for a free slot...
      final String containerType = executor.getContainerType();
      signalPossiblyWaitingScheduler(containerType);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void onTaskGroupExecutionFailed(final String executorId, final String taskGroupId) {
    lock.lock();
    try {
      final ExecutorRepresenter executor = executorRegistry.getExecutorRepresenter(executorId);

      executor.onTaskGroupExecutionFailed(taskGroupId);
      LOG.info("{" + taskGroupId + "} failed in [" + executorId + "]");

      // the scheduler thread may be waiting for a free slot...
      final String containerType = executor.getContainerType();
      signalPossiblyWaitingScheduler(containerType);
    } finally {
      lock.unlock();
    }
  }
}
