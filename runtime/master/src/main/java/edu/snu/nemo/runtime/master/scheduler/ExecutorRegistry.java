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

import com.google.common.annotations.VisibleForTesting;
import edu.snu.nemo.common.Pair;
import edu.snu.nemo.runtime.common.plan.physical.ScheduledTask;
import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * (WARNING) This class must be thread-safe.
 * Maintains map between executor id and {@link ExecutorRepresenter}.
 */
@DriverSide
@ThreadSafe
public final class ExecutorRegistry {
  /**
   * States of an executor.
   */
  enum ExecutorState {
    RUNNING,
    FAILED,
    COMPLETED
  }

  private final Map<String, Pair<ExecutorRepresenter, ExecutorState>> executors;

  @Inject
  public ExecutorRegistry() {
    this.executors = new HashMap<>();
  }

  synchronized void registerExecutor(final ExecutorRepresenter executor) {
    final String executorId = executor.getExecutorId();
    if (executors.containsKey(executorId)) {
      throw new IllegalArgumentException("Duplicate executor: " + executor.toString());
    } else {
      executors.put(executorId, Pair.of(executor, ExecutorState.RUNNING));
    }
  }

  synchronized boolean registerTask(final SchedulingPolicy policy, final ScheduledTask task) {
    final Set<ExecutorRepresenter> candidateExecutors =
        policy.filterExecutorRepresenters(getRunningExecutors(), task);
    final Optional<ExecutorRepresenter> firstCandiate = candidateExecutors.stream().findFirst();

    if (firstCandiate.isPresent()) {
      final ExecutorRepresenter selectedExecutor = firstCandiate.get();
      selectedExecutor.onTaskScheduled(task);
      return true;
    } else {
      return false;
    }
  }

  synchronized boolean updateExecutor(
      final String executorId,
      final BiFunction<ExecutorRepresenter, ExecutorState, Pair<ExecutorRepresenter, ExecutorState>> updater) {
    final Pair<ExecutorRepresenter, ExecutorState> pair = executors.get(executorId);
    if (pair == null) {
      return false;
    } else {
      executors.put(executorId, updater.apply(pair.left(), pair.right()));
      return true;
    }
  }

  synchronized void terminate() {
    for (final ExecutorRepresenter executor : getRunningExecutors()) {
      executor.shutDown();
      executors.put(executor.getExecutorId(), Pair.of(executor, ExecutorState.COMPLETED));
    }
  }

  /**
   * Retrieves the executor to which the given task was scheduled.
   * @param taskId of the task to search.
   * @return the {@link ExecutorRepresenter} of the executor the task was scheduled to.
   */
  @VisibleForTesting
  synchronized Optional<ExecutorRepresenter> findExecutorForTask(final String taskId) {
    for (final ExecutorRepresenter executor : getRunningExecutors()) {
      if (executor.getRunningTasks().contains(taskId) || executor.getCompleteTasks().contains(taskId)) {
        return Optional.of(executor);
      }
    }
    return Optional.empty();
  }

  private Set<ExecutorRepresenter> getRunningExecutors() {
    return executors.values()
        .stream()
        .filter(pair -> pair.right().equals(ExecutorState.RUNNING))
        .map(Pair::left)
        .collect(Collectors.toSet());
  }

  @Override
  public String toString() {
    return executors.toString();
  }
}
