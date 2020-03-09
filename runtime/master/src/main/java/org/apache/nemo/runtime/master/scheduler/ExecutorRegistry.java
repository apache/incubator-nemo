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
package org.apache.nemo.runtime.master.scheduler;

import com.google.common.annotations.VisibleForTesting;
import org.apache.nemo.common.Pair;
import org.apache.nemo.runtime.common.plan.Task;
import org.apache.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
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
    TERMINATED
  }

  private final Map<String, Pair<ExecutorRepresenter, ExecutorState>> executors;

  @Inject
  private ExecutorRegistry() {
    this.executors = new HashMap<>();
  }

  /**
   * Static constructor for manual usage.
   * @return a new instance of ExecutorRegistry.
   */
  public static ExecutorRegistry newInstance() {
    return new ExecutorRegistry();
  }

  synchronized void registerExecutor(final ExecutorRepresenter executor) {
    final String executorId = executor.getExecutorId();
    if (executors.containsKey(executorId)) {
      throw new IllegalArgumentException("Duplicate executor: " + executor.toString());
    } else {
      executors.put(executorId, Pair.of(executor, ExecutorState.RUNNING));
    }
  }

  public synchronized void viewExecutors(final Consumer<Set<ExecutorRepresenter>> consumer) {
    consumer.accept(getRunningExecutors());
  }

  synchronized void updateExecutor(
    final String executorId,
    final BiFunction<ExecutorRepresenter, ExecutorState, Pair<ExecutorRepresenter, ExecutorState>> updater) {
    final Pair<ExecutorRepresenter, ExecutorState> pair = executors.get(executorId);
    if (pair == null) {
      throw new IllegalArgumentException("Unknown executor id " + executorId);
    } else {
      executors.put(executorId, updater.apply(pair.left(), pair.right()));
    }
  }

  synchronized void terminate() {
    for (final ExecutorRepresenter executor : getRunningExecutors()) {
      executor.shutDown();
      executors.put(executor.getExecutorId(), Pair.of(executor, ExecutorState.TERMINATED));
    }
  }

  /**
   * Retrieves the executor to which the given task was scheduled.
   *
   * @param taskId of the task to search.
   * @return the {@link ExecutorRepresenter} of the executor the task was scheduled to.
   */
  @VisibleForTesting
  synchronized Optional<ExecutorRepresenter> findExecutorForTask(final String taskId) {
    for (final ExecutorRepresenter executor : getRunningExecutors()) {
      for (final Task runningTask : executor.getRunningTasks()) {
        if (runningTask.getTaskId().equals(taskId)) {
          return Optional.of(executor);
        }
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
