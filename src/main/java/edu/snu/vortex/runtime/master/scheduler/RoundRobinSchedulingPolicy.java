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
package edu.snu.vortex.runtime.master.scheduler;

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.vortex.runtime.exception.SchedulingException;
import edu.snu.vortex.runtime.master.resource.ContainerManager;
import edu.snu.vortex.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@inheritDoc}
 * A Round-Robin implementation used by {@link BatchScheduler}.
 *
 * This policy keeps a list of available {@link ExecutorRepresenter} for each type of container.
 * The RR policy is used for each container type when trying to schedule a task group.
 */
@ThreadSafe
public final class RoundRobinSchedulingPolicy implements SchedulingPolicy {
  private static final Logger LOG = Logger.getLogger(RoundRobinSchedulingPolicy.class.getName());

  private final ContainerManager containerManager;

  private final int scheduleTimeoutMs;

  /**
   * Thread safety is provided by this lock as multiple threads can call the methods in this class concurrently.
   */
  private final Lock lock;

  /**
   * Executor allocation is achieved by putting conditions for each container type.
   * The condition blocks when there is no executor of the container type available,
   * and is released when such an executor becomes available (either by an extra executor, or a task group completion).
   */
  private final Map<RuntimeAttribute, Condition> attemptToScheduleByContainerType;

  /**
   * The pool of executors available for each container type.
   */
  private final Map<RuntimeAttribute, List<String>> executorIdByContainerType;

  /**
   * A copy of {@link ContainerManager#executorRepresenterMap}.
   * This cached copy is updated when an executor is added or removed.
   */
  private final Map<String, ExecutorRepresenter> executorRepresenterMap;

  /**
   * The index of the next executor to be assigned for each container type.
   * This map allows the executor index computation of the RR scheduling.
   */
  private final Map<RuntimeAttribute, Integer> nextExecutorIndexByContainerType;

  @Inject
  public RoundRobinSchedulingPolicy(final ContainerManager containerManager,
                                    @Parameter(JobConf.SchedulerTimeoutMs.class) final int scheduleTimeoutMs) {
    this.containerManager = containerManager;
    this.scheduleTimeoutMs = scheduleTimeoutMs;
    this.lock = new ReentrantLock();
    this.executorIdByContainerType = new HashMap<>();
    this.executorRepresenterMap = new HashMap<>();
    this.attemptToScheduleByContainerType = new HashMap<>();
    this.nextExecutorIndexByContainerType = new HashMap<>();
  }

  public long getScheduleTimeoutMs() {
    return scheduleTimeoutMs;
  }

  @Override
  public Optional<String> attemptSchedule(final ScheduledTaskGroup scheduledTaskGroup) {
    lock.lock();
    try {
      final RuntimeAttribute containerType = scheduledTaskGroup.getTaskGroup().getContainerType();
      String executorId = selectExecutorByRR(containerType);
      if (executorId == null) { // If there is no available executor to schedule this task group now,

        // we must wait until an executor becomes available, by putting a condition on the lock.
        Condition attemptToSchedule = attemptToScheduleByContainerType.get(containerType);
        if (attemptToSchedule == null) { // initialize a new condition if this containerType has never waited before.
          attemptToSchedule = lock.newCondition();
          attemptToScheduleByContainerType.put(containerType, attemptToSchedule);
        }
        boolean executorAvailable = attemptToSchedule.await(scheduleTimeoutMs, TimeUnit.MILLISECONDS);

        if (executorAvailable) { // if an executor has become available before scheduleTimeoutMs,
          executorId = selectExecutorByRR(containerType);
          return Optional.of(executorId);
        } else {
          return Optional.empty();
        }
      } else {
        return Optional.of(executorId);
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
   * @param containerType to select an executor for.
   * @return the selected executor.
   */
  private String selectExecutorByRR(final RuntimeAttribute containerType) {
    String selectedExecutorId = null;
    final List<String> executorIds = executorIdByContainerType.get(containerType);

    if (executorIds != null && !executorIds.isEmpty()) {
      final int numExecutors = executorIds.size();
      int nextExecutorIndex = nextExecutorIndexByContainerType.get(containerType);
      for (int i = 0; i < numExecutors; i++) {
        final int index = (nextExecutorIndex + i) % numExecutors;
        selectedExecutorId = executorIds.get(index);

        final ExecutorRepresenter executor = executorRepresenterMap.get(selectedExecutorId);
        if (executor.getRunningTaskGroups().size() < executor.getExecutorCapacity()) {
          nextExecutorIndex = (index + 1) % numExecutors;
          nextExecutorIndexByContainerType.put(containerType, nextExecutorIndex);
          break;
        } else {
          selectedExecutorId = null;
        }
      }
    }
    return selectedExecutorId;
  }

  @Override
  public void onExecutorAdded(final String executorId) {
    lock.lock();
    try {
      updateCachedExecutorRepresenterMap();
      final ExecutorRepresenter executor = executorRepresenterMap.get(executorId);
      final RuntimeAttribute containerType = executor.getContainerType();
      final List<String> executors = executorIdByContainerType.putIfAbsent(containerType, new ArrayList<>());

      if (executors == null) { // This container type is initially being introduced.
        executorIdByContainerType.get(containerType).add(executorId);
        nextExecutorIndexByContainerType.put(containerType, 0);
        attemptToScheduleByContainerType.put(containerType, lock.newCondition());
      } else { // This container type has been introduced and there may be a TaskGroup waiting to be scheduled.
        executorIdByContainerType.get(containerType)
            .add(nextExecutorIndexByContainerType.get(containerType), executorId);
        attemptToScheduleByContainerType.get(containerType).signal();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Set<String> onExecutorRemoved(final String executorId) {
    lock.lock();
    try {
      final ExecutorRepresenter executor = executorRepresenterMap.get(executorId);
      final RuntimeAttribute containerType = executor.getContainerType();

      final List<String> executorIdList = executorIdByContainerType.get(containerType);
      int nextExecutorIndex = nextExecutorIndexByContainerType.get(containerType);

      final int executorAssignmentLocation = executorIdList.indexOf(executorId);
      if (executorAssignmentLocation < nextExecutorIndex) {
        nextExecutorIndexByContainerType.put(containerType, nextExecutorIndex - 1);
      } else if (executorAssignmentLocation == nextExecutorIndex) {
        nextExecutorIndexByContainerType.put(containerType, 0);
      }
      executorIdList.remove(executorId);
      updateCachedExecutorRepresenterMap();

      return executor.getRunningTaskGroups();
    } finally {
      lock.unlock();
    }
  }

  private void updateCachedExecutorRepresenterMap() {
    executorRepresenterMap.clear();
    executorRepresenterMap.putAll(containerManager.getExecutorRepresenterMap());
  }

  @Override
  public void onTaskGroupScheduled(final String executorId, final ScheduledTaskGroup scheduledTaskGroup) {
    lock.lock();
    try {
      final ExecutorRepresenter executor = executorRepresenterMap.get(executorId);
      LOG.log(Level.INFO, "Scheduling {0} to {1}",
          new Object[]{scheduledTaskGroup.getTaskGroup().getTaskGroupId(), executorId});
      executor.onTaskGroupScheduled(scheduledTaskGroup);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void onTaskGroupExecutionComplete(final String executorId, final String taskGroupId) {
    lock.lock();
    try {
      final ExecutorRepresenter executor = executorRepresenterMap.get(executorId);
      final RuntimeAttribute containerType = executor.getContainerType();
      executor.onTaskGroupExecutionComplete(taskGroupId);
      LOG.log(Level.INFO, "Completed {" + taskGroupId + "} in ["
          + executorId + "]");
      attemptToScheduleByContainerType.get(containerType).signal();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void onTaskGroupExecutionFailed(final String executorId, final String taskGroupId) {
    // TODO #163: Handle Fault Tolerance
  }
}
