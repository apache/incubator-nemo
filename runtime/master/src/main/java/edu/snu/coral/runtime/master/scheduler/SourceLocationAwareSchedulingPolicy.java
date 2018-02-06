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
package edu.snu.coral.runtime.master.scheduler;

import edu.snu.coral.common.dag.DAG;
import edu.snu.coral.common.ir.Readable;
import edu.snu.coral.runtime.common.plan.RuntimeEdge;
import edu.snu.coral.runtime.common.plan.physical.BoundedSourceTask;
import edu.snu.coral.runtime.common.plan.physical.ScheduledTaskGroup;
import edu.snu.coral.runtime.common.plan.physical.Task;
import edu.snu.coral.runtime.common.state.TaskGroupState;
import edu.snu.coral.runtime.master.JobStateManager;
import edu.snu.coral.runtime.master.resource.ContainerManager;
import edu.snu.coral.runtime.master.resource.ExecutorRepresenter;
import org.apache.commons.lang.SerializationUtils;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * This policy is same as {@link RoundRobinSchedulingPolicy}, however for TaskGroups
 * with {@link edu.snu.coral.common.ir.vertex.SourceVertex}, it tries to pick one of the executors
 * where the corresponding data resides.
 */
@ThreadSafe
@DriverSide
public final class SourceLocationAwareSchedulingPolicy implements SchedulingPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(SourceLocationAwareSchedulingPolicy.class);

  private final ContainerManager containerManager;
  private final RoundRobinSchedulingPolicy roundRobinSchedulingPolicy;
  private final long scheduleTimeoutMs;
  private final Set<String> availableExecutors = new HashSet<>();

  @Inject
  private SourceLocationAwareSchedulingPolicy(final ContainerManager containerManager,
                                              final RoundRobinSchedulingPolicy roundRobinSchedulingPolicy) {
    this.containerManager = containerManager;
    this.roundRobinSchedulingPolicy = roundRobinSchedulingPolicy;
    this.scheduleTimeoutMs = roundRobinSchedulingPolicy.getScheduleTimeoutMs();
  }

  @Override
  public long getScheduleTimeoutMs() {
    return scheduleTimeoutMs;
  }

  @Override
  public synchronized boolean scheduleTaskGroup(final ScheduledTaskGroup scheduledTaskGroup,
                                                final JobStateManager jobStateManager) {
    final DAG<Task, RuntimeEdge<Task>> taskGroupDAG = (DAG<Task, RuntimeEdge<Task>>)
        SerializationUtils.deserialize(scheduledTaskGroup.getSerializedTaskGroupDag());
    Set<String> sourceLocations = Collections.emptySet();
    try {
      sourceLocations = getSourceLocation(taskGroupDAG, scheduledTaskGroup.getLogicalTaskIdToReadable());
    } catch (final Exception e) {
      LOG.warn(String.format("Cannot get source location for %s", scheduledTaskGroup.getTaskGroupId()), e);
    }
    if (sourceLocations.size() == 0) {
      return roundRobinSchedulingPolicy.scheduleTaskGroup(scheduledTaskGroup, jobStateManager);
    }

    return attemptSchedule(scheduledTaskGroup, jobStateManager, sourceLocations);
  }

  /**
   * Try to schedule a TaskGroup with source task.
   * @param scheduledTaskGroup TaskGroup to schedule
   * @param jobStateManager {@link JobStateManager}
   * @param jobStateManager jobStateManager which the TaskGroup belongs to.
   * @return true if the task group is successfully scheduled, false otherwise.
   */
  private synchronized boolean attemptSchedule(final ScheduledTaskGroup scheduledTaskGroup,
                                               final JobStateManager jobStateManager,
                                               final Set<String> sourceLocations) {
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
  }

  @Override
  public synchronized void onExecutorAdded(final String executorId) {
    availableExecutors.add(executorId);
    roundRobinSchedulingPolicy.onExecutorAdded(executorId);
  }

  @Override
  public synchronized Set<String> onExecutorRemoved(final String executorId) {
    availableExecutors.remove(executorId);
    return roundRobinSchedulingPolicy.onExecutorRemoved(executorId);
  }

  @Override
  public void onTaskGroupExecutionComplete(final String executorId, final String taskGroupId) {
    roundRobinSchedulingPolicy.onTaskGroupExecutionComplete(executorId, taskGroupId);
  }

  @Override
  public void onTaskGroupExecutionFailed(final String executorId, final String taskGroupId) {
    roundRobinSchedulingPolicy.onTaskGroupExecutionFailed(executorId, taskGroupId);
  }

  /**
   * @param containerType type of the desired container type
   * @param nodeNames set of node names
   * @return list of executors, which resides in one of {@code nodeNames}, has container type of {@code containerType},
   *         and has an empty slot for execution
   */
  private synchronized List<ExecutorRepresenter> selectExecutorByContainerTypeAndNodeNames(
      final String containerType, final Set<String> nodeNames) {
    LOG.info("Executor predicate: {}, [{}] from [{}]", containerType, String.join(", ", nodeNames),
        String.join(", ", availableExecutors));
    final Map<String, ExecutorRepresenter> executorIdToExecutorRepresenter
        = containerManager.getExecutorRepresenterMap();
    return availableExecutors.stream().map(executorId -> executorIdToExecutorRepresenter.get(executorId))
        .filter(executor -> executor.getContainerType().equals(containerType))
        .filter(executor -> executor.getRunningTaskGroups().size() < executor.getExecutorCapacity())
        .filter(executor -> nodeNames.contains(executor.getNodeName()))
        .collect(Collectors.toList());
  }

  /**
   * @param taskGroupDAG TaskGroup DAG to investigate
   * @param logicalTaskIdToReadable the map between logical task id to {@link Readable}.
   * @return Set of source locations from source tasks in {@code taskGroupDAG}
   * @throws Exception for any exception raised during querying source locations for a readable
   */
  private static Set<String> getSourceLocation(final DAG<Task, RuntimeEdge<Task>> taskGroupDAG,
                                               final Map<String, Readable> logicalTaskIdToReadable) throws Exception {
    final List<String> sourceLocations = new ArrayList<>();
    final List<BoundedSourceTask> sourceTasks = taskGroupDAG.getVertices().stream()
        .filter(task -> task instanceof BoundedSourceTask)
        .map(task -> ((BoundedSourceTask) task))
        .collect(Collectors.toList());
    for (final BoundedSourceTask sourceTask : sourceTasks) {
      final Readable readable = logicalTaskIdToReadable.get(sourceTask.getId());
      final Collection<String> locations = readable.getLocations();
      sourceLocations.addAll(locations);
    }
    return new HashSet<>(sourceLocations);
  }
}
