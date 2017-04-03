/*
 * Copyright (C) 2016 Seoul National University
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

import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.plan.physical.TaskGroup;
import edu.snu.vortex.runtime.exception.SchedulingException;
import edu.snu.vortex.runtime.master.ExecutorRepresenter;

import java.util.*;
import java.util.concurrent.*;

/**
 * {@inheritDoc}
 * A sample implementation.
 *
 * This scheduler simply keeps all {@link ExecutorRepresenter} available for each type of resource.
 * It simply assigns the first available executor in the queue for the resource type the task group must run on.
 */
public final class SampleScheduler implements SchedulingPolicy {
  private static SampleScheduler instance;

  private final long scheduleTimeout = 2 * 1000;
  private final ConcurrentMap<RuntimeAttribute, BlockingQueue<ExecutorRepresenter>> executorByResourceType;

  public static SampleScheduler newInstance() {
    if (instance == null) {
      instance = new SampleScheduler();
    }
    return instance;
  }

  private SampleScheduler() {
    this.executorByResourceType = new ConcurrentHashMap<>();
  }

  @Override
  public long getScheduleTimeout() {
    return scheduleTimeout;
  }

  @Override
  public Optional<String> attemptSchedule(final TaskGroup taskGroup) {
    try {
      final RuntimeAttribute resourceType = taskGroup.getResourceType();

      // This call to a LinkedBlockingQueue waits if there is none available for the given timeout.
      // Notice that this behavior matches the description given in SchedulerPolicy interface javadoc.
      final ExecutorRepresenter executor = executorByResourceType.get(resourceType)
          .poll(scheduleTimeout, TimeUnit.MILLISECONDS);
      if (executor == null) {
        return Optional.empty();
      } else {
        return Optional.of(executor.getExecutorId());
      }
    } catch (final Exception e) {
      throw new SchedulingException(e.getMessage());
    }
  }

  @Override
  public void onExecutorAdded(final ExecutorRepresenter executor) {
    // Adding the executor to the LinkedBlockingQueue frees attemptSchedule() from blocking
    // with the executor offered to the queue as a result of this method call.
    final RuntimeAttribute resourceType = executor.getResourceType();
    executorByResourceType.putIfAbsent(resourceType, new LinkedBlockingQueue<>());
    executorByResourceType.get(resourceType).offer(executor);
  }

  @Override
  public void onExecutorDeleted(final ExecutorRepresenter executor) {
    // Removing the executor from the LinkedBlockingQueue leaves attemptSchedule() blocking
    // if the queue becomes empty as a result of this method call.
    final RuntimeAttribute resourceType = executor.getResourceType();
    executorByResourceType.get(resourceType).remove(executor);
  }

  @Override
  public void onTaskGroupScheduled(final ExecutorRepresenter executor, final TaskGroup taskGroup) {
    // Removing the executor from the LinkedBlockingQueue leaves attemptSchedule() blocking
    // if the queue becomes empty as a result of this method call.
    final RuntimeAttribute resourceType = executor.getResourceType();
    executorByResourceType.get(resourceType).remove(executor);
  }

  @Override
  public void onTaskGroupLaunched(final ExecutorRepresenter executor, final TaskGroup taskGroup) {
    // Tentative. Do nothing.
  }

  @Override
  public void onTaskGroupExecutionComplete(final ExecutorRepresenter executor, final TaskGroup taskGroup) {
    // Adding the executor to the LinkedBlockingQueue frees attemptSchedule() from blocking
    // with the executor offered to the queue as a result of this method call.
    final RuntimeAttribute resourceType = executor.getResourceType();
    executorByResourceType.get(resourceType).offer(executor);
  }
}
