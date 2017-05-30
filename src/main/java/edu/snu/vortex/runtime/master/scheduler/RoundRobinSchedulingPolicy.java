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
import edu.snu.vortex.runtime.master.ExecutorRepresenter;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * {@inheritDoc}
 * A Round-Robin implementation used by {@link BatchScheduler}.
 *
 * This policy keeps a list of available {@link ExecutorRepresenter} for each type of resource.
 * The RR policy is used for each resource type when trying to schedule a task group.
 */
@ThreadSafe
public final class RoundRobinSchedulingPolicy implements SchedulingPolicy {
  private static final Logger LOG = Logger.getLogger(RoundRobinSchedulingPolicy.class.getName());

  private final int scheduleTimeoutMs;

  /**
   * Thread safety is provided by this lock as multiple threads can call the methods in this class concurrently.
   */
  private final Lock lock;

  /**
   * Executor allocation is achieved by putting conditions for each resource type.
   * The condition blocks when there is no executor of the resource type available,
   * and is released when such an executor becomes available (either by an extra executor, or a task group completion).
   */
  private final Map<RuntimeAttribute, Condition> attemptToScheduleByResourceType;

  /**
   * The pool of executors available for each resource type.
   */
  // TODO #233: Introduce Container Manager
  private final Map<RuntimeAttribute, List<ExecutorRepresenter>> executorByResourceType;

  /**
   * The index of the next executor to be assigned for each resource type.
   * This map allows the executor index computation of the RR scheduling.
   */
  private final Map<RuntimeAttribute, Integer> nextExecutorIndexByResourceType;

  @Inject
  public RoundRobinSchedulingPolicy(@Parameter(JobConf.SchedulerTimeoutMs.class) final int scheduleTimeoutMs) {
    this.scheduleTimeoutMs = scheduleTimeoutMs;
    this.lock = new ReentrantLock();
    this.executorByResourceType = new HashMap<>();
    this.attemptToScheduleByResourceType = new HashMap<>();
    this.nextExecutorIndexByResourceType = new HashMap<>();
  }

  public long getScheduleTimeoutMs() {
    return scheduleTimeoutMs;
  }

  @Override
  public Optional<ExecutorRepresenter> attemptSchedule(final ScheduledTaskGroup scheduledTaskGroup) {
    lock.lock();
    try {
      final RuntimeAttribute resourceType = scheduledTaskGroup.getTaskGroup().getResourceType();
      ExecutorRepresenter executor = selectExecutorByRR(resourceType);
      if (executor == null) { // If there is no available executor to schedule this task group now,

        // we must wait until an executor becomes available, by putting a condition on the lock.
        Condition attemptToSchedule = attemptToScheduleByResourceType.get(resourceType);
        if (attemptToSchedule == null) { // initialize a new condition if this resourceType has never waited before.
          attemptToSchedule = lock.newCondition();
          attemptToScheduleByResourceType.put(resourceType, attemptToSchedule);
        }
        boolean executorAvailable = attemptToSchedule.await(scheduleTimeoutMs, TimeUnit.MILLISECONDS);

        if (executorAvailable) { // if an executor has become available before scheduleTimeoutMs,
          executor = selectExecutorByRR(resourceType);
          return Optional.of(executor);
        } else {
          return Optional.empty();
        }
      } else {
        return Optional.of(executor);
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
   * @param resourceType to select an executor for.
   * @return the selected executor.
   */
  private ExecutorRepresenter selectExecutorByRR(final RuntimeAttribute resourceType) {
    ExecutorRepresenter selectedExecutor = null;
    final List<ExecutorRepresenter> executorRepresenterList = executorByResourceType.get(resourceType);

    if (executorRepresenterList != null && !executorRepresenterList.isEmpty()) {
      final int numExecutors = executorRepresenterList.size();
      int nextExecutorIndex = nextExecutorIndexByResourceType.get(resourceType);
      for (int i = 0; i < numExecutors; i++) {
        final int index = (nextExecutorIndex + i) % numExecutors;
        selectedExecutor = executorRepresenterList.get(index);

        if (selectedExecutor.getRunningTaskGroups().size() < selectedExecutor.getExecutorCapacity()) {
          nextExecutorIndex = (index + 1) % numExecutors;
          nextExecutorIndexByResourceType.put(resourceType, nextExecutorIndex);
          break;
        } else {
          selectedExecutor = null;
        }
      }
    }
    return selectedExecutor;
  }

  @Override
  public void onExecutorAdded(final ExecutorRepresenter executor) {
    lock.lock();
    try {
      final RuntimeAttribute resourceType = executor.getResourceType();
      final List<ExecutorRepresenter> executors =
          executorByResourceType.putIfAbsent(resourceType, new ArrayList<>());

      if (executors == null) { // This resource type is initially being introduced.
        executorByResourceType.get(resourceType).add(executor);
        nextExecutorIndexByResourceType.put(resourceType, 0);
        attemptToScheduleByResourceType.put(resourceType, lock.newCondition());
      } else { // This resource type has been introduced and there may be a TaskGroup waiting to be scheduled.
        executorByResourceType.get(resourceType).add(nextExecutorIndexByResourceType.get(resourceType), executor);
        attemptToScheduleByResourceType.get(resourceType).signal();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Set<String> onExecutorRemoved(final ExecutorRepresenter executor) {
    lock.lock();
    try {
      final RuntimeAttribute resourceType = executor.getResourceType();

      final List<ExecutorRepresenter> executorRepresenterList = executorByResourceType.get(resourceType);
      int nextExecutorIndex = nextExecutorIndexByResourceType.get(resourceType);

      final int executorAssignmentLocation = executorRepresenterList.indexOf(executor);
      if (executorAssignmentLocation < nextExecutorIndex) {
        nextExecutorIndexByResourceType.put(resourceType, nextExecutorIndex - 1);
      } else if (executorAssignmentLocation == nextExecutorIndex) {
        nextExecutorIndexByResourceType.put(resourceType, 0);
      }

      executorRepresenterList.remove(executor);
    } finally {
      lock.unlock();
    }
    return executor.getRunningTaskGroups();
  }

  @Override
  public void onTaskGroupScheduled(final ExecutorRepresenter executor, final ScheduledTaskGroup scheduledTaskGroup) {
    lock.lock();
    try {
      executor.onTaskGroupScheduled(scheduledTaskGroup);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void onTaskGroupExecutionComplete(final ExecutorRepresenter executor, final String taskGroupId) {
    lock.lock();
    try {
      final RuntimeAttribute resourceType = executor.getResourceType();
      executor.onTaskGroupExecutionComplete(taskGroupId);
      attemptToScheduleByResourceType.get(resourceType).signal();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void onTaskGroupExecutionFailed(final ExecutorRepresenter executor, final String taskGroupId) {
    // TODO #163: Handle Fault Tolerance
  }
}
