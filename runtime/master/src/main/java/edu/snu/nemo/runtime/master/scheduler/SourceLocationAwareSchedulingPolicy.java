/*
 * Copyright (C) 2018 Seoul National University
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

import edu.snu.nemo.common.exception.SchedulingException;
import edu.snu.nemo.common.ir.Readable;
import edu.snu.nemo.common.ir.vertex.executionproperty.ExecutorPlacementProperty;
import edu.snu.nemo.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.nemo.runtime.common.state.TaskGroupState;
import edu.snu.nemo.runtime.master.JobStateManager;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This policy is same as {@link RoundRobinSchedulingPolicy}, however for TaskGroups
 * with {@link edu.snu.nemo.common.ir.vertex.SourceVertex}, it tries to pick one of the executors
 * where the corresponding data resides.
 */
@ThreadSafe
@DriverSide
public final class SourceLocationAwareSchedulingPolicy implements SchedulingPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(SourceLocationAwareSchedulingPolicy.class);

  private final ExecutorRegistry executorRegistry;
  private final RoundRobinSchedulingPolicy roundRobinSchedulingPolicy;
  private final long scheduleTimeoutMs;
  private final Lock lock = new ReentrantLock();
  private final Condition moreExecutorsAvailableCondition = lock.newCondition();

  /**
   * Injectable constructor for {@link SourceLocationAwareSchedulingPolicy}.
   * @param executorRegistry provides catalog of available executors
   * @param roundRobinSchedulingPolicy provides fallback for TaskGroups with no input source information
   */
  @Inject
  private SourceLocationAwareSchedulingPolicy(final ExecutorRegistry executorRegistry,
                                              final RoundRobinSchedulingPolicy roundRobinSchedulingPolicy) {
    this.executorRegistry = executorRegistry;
    this.roundRobinSchedulingPolicy = roundRobinSchedulingPolicy;
    this.scheduleTimeoutMs = roundRobinSchedulingPolicy.getScheduleTimeoutMs();
  }

  @Override
  public long getScheduleTimeoutMs() {
    return scheduleTimeoutMs;
  }

  /**
   * Try to schedule a TaskGroup.
   * If the TaskGroup has one or more source tasks, this method schedules the task group to one of the physical nodes,
   * chosen from union of set of locations where splits of each source task resides.
   * If the TaskGroup has no source tasks, falls back to {@link RoundRobinSchedulingPolicy}.
   * @param scheduledTaskGroup to schedule.
   * @param jobStateManager jobStateManager which the TaskGroup belongs to.
   * @return true if the task group is successfully scheduled, false otherwise.
   */
  @Override
  public boolean scheduleTaskGroup(final ScheduledTaskGroup scheduledTaskGroup,
                                   final JobStateManager jobStateManager) {
    lock.lock();
    try {
      Set<String> sourceLocations = Collections.emptySet();
      try {
        sourceLocations = getSourceLocations(scheduledTaskGroup.getLogicalTaskIdToReadable().values());
      } catch (final UnsupportedOperationException e) {
        // do nothing
      } catch (final Exception e) {
        LOG.warn(String.format("Exception while trying to get source location for %s",
            scheduledTaskGroup.getTaskGroupId()), e);
      }
      if (sourceLocations.size() == 0) {
        // No source location information found, fall back to the RoundRobinSchedulingPolicy
        return roundRobinSchedulingPolicy.scheduleTaskGroup(scheduledTaskGroup, jobStateManager);
      }

      long timeoutInNanoseconds = scheduleTimeoutMs * 1000000;
      while (timeoutInNanoseconds > 0) {
        if (scheduleToLocalNode(scheduledTaskGroup, jobStateManager, sourceLocations)) {
          return true;
        }
        try {
          timeoutInNanoseconds = moreExecutorsAvailableCondition.awaitNanos(timeoutInNanoseconds);
          // Signals on this condition does not necessarily guarantee that the added executor helps scheduling the
          // TaskGroup we are interested in. We need to await again if the consequent scheduling attempt still fails,
          // until we spend the time budget specified.
        } catch (final InterruptedException e) {
          throw new SchedulingException(e);
        }
      }
      return false;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Try to schedule a TaskGroup with source task.
   * @param scheduledTaskGroup TaskGroup to schedule
   * @param jobStateManager {@link JobStateManager}
   * @param jobStateManager jobStateManager which the TaskGroup belongs to.
   * @return true if the task group is successfully scheduled, false otherwise.
   */
  private boolean scheduleToLocalNode(final ScheduledTaskGroup scheduledTaskGroup,
                                      final JobStateManager jobStateManager,
                                      final Set<String> sourceLocations) {
    lock.lock();
    try {
      final List<ExecutorRepresenter> candidateExecutors =
          selectExecutorByContainerTypeAndNodeNames(scheduledTaskGroup.getContainerType(), sourceLocations);
      if (candidateExecutors.size() == 0) {
        return false;
      }
      final int randomIndex = ThreadLocalRandom.current().nextInt(0, candidateExecutors.size());
      final ExecutorRepresenter selectedExecutor = candidateExecutors.get(randomIndex);

      jobStateManager.onTaskGroupStateChanged(scheduledTaskGroup.getTaskGroupId(), TaskGroupState.State.EXECUTING);
      selectedExecutor.onTaskGroupScheduled(scheduledTaskGroup);
      LOG.info("Scheduling {} (source location: {}) to {} (node name: {})", scheduledTaskGroup.getTaskGroupId(),
          String.join(", ", sourceLocations), selectedExecutor.getExecutorId(),
          selectedExecutor.getNodeName());
      return true;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void onExecutorAdded(final ExecutorRepresenter executorRepresenter) {
    lock.lock();
    try {
      moreExecutorsAvailableCondition.signal();
      roundRobinSchedulingPolicy.onExecutorAdded(executorRepresenter);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Set<String> onExecutorRemoved(final String executorId) {
    lock.lock();
    try {
      return roundRobinSchedulingPolicy.onExecutorRemoved(executorId);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void onTaskGroupExecutionComplete(final String executorId, final String taskGroupId) {
    lock.lock();
    try {
      moreExecutorsAvailableCondition.signal();
      roundRobinSchedulingPolicy.onTaskGroupExecutionComplete(executorId, taskGroupId);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void onTaskGroupExecutionFailed(final String executorId, final String taskGroupId) {
    lock.lock();
    try {
      moreExecutorsAvailableCondition.signal();
      roundRobinSchedulingPolicy.onTaskGroupExecutionFailed(executorId, taskGroupId);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void terminate() {
    lock.lock();
    try {
      roundRobinSchedulingPolicy.terminate();
    } finally {
      lock.unlock();
    }
  }

  /**
   * @param containerType type of the desired container type
   * @param nodeNames set of node names
   * @return list of executors, which resides in one of {@code nodeNames}, has container type of {@code containerType},
   *         and has an empty slot for execution
   */
  private List<ExecutorRepresenter> selectExecutorByContainerTypeAndNodeNames(
      final String containerType, final Set<String> nodeNames) {
    lock.lock();
    try {
      final Stream<ExecutorRepresenter> localNodesWithSpareCapacity = executorRegistry.getRunningExecutorIds().stream()
          .map(executorId -> executorRegistry.getRunningExecutorRepresenter(executorId))
          .filter(executor -> executor.getRunningTaskGroups().size() < executor.getExecutorCapacity())
          .filter(executor -> nodeNames.contains(executor.getNodeName()));
      if (containerType.equals(ExecutorPlacementProperty.NONE)) {
        return localNodesWithSpareCapacity.collect(Collectors.toList());
      } else {
        return localNodesWithSpareCapacity.filter(executor -> executor.getContainerType().equals(containerType))
            .collect(Collectors.toList());
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * @param readables collection of readables
   * @return Set of source locations from source tasks in {@code taskGroupDAG}
   * @throws Exception for any exception raised during querying source locations for a readable
   */
  private static Set<String> getSourceLocations(final Collection<Readable> readables) throws Exception {
    final List<String> sourceLocations = new ArrayList<>();
    for (final Readable readable : readables) {
      sourceLocations.addAll(readable.getLocations());
    }
    return new HashSet<>(sourceLocations);
  }
}
