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
import org.apache.nemo.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.comm.ControlMessage;
import org.apache.nemo.common.Task;
import org.apache.nemo.runtime.master.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.nemo.runtime.message.MessageEnvironment.ListenerType.EXECUTOR_MESSAGE_LISTENER_ID;

/**
 * (WARNING) This class must be thread-safe.
 */
@DriverSide
@ThreadSafe
public final class ExecutorRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorRegistry.class.getName());

  /**
   * States of an executor.
   */
  enum ExecutorState {
    RUNNING,
    FAILED,
    TERMINATED
  }

  private final Map<String, Pair<ExecutorRepresenter, ExecutorState>> executors;
  private final Set<String> lambdaExecutors;
  private final Set<String> activatedLambdaTasks;

  @Inject
  private ExecutorRegistry() {
    this.executors = new ConcurrentHashMap<>();
    this.lambdaExecutors = new ConcurrentSkipListSet<>();
    this.activatedLambdaTasks = new ConcurrentSkipListSet<>();
  }

  public synchronized void registerExecutor(final ExecutorRepresenter executor) {
    final String executorId = executor.getExecutorId();
    if (executors.containsKey(executorId)) {
      throw new IllegalArgumentException("Duplicate executor: " + executor.toString());
    } else {
      // broadcast executors

      // 1. First, send new executor to others
      executors.values().forEach(val -> {

        final Set<String> existingExecutors =
          executors.values().stream().map(e -> e.left().getExecutorId())
            .collect(Collectors.toSet());
        existingExecutors.remove(val.left().getExecutorId());
        existingExecutors.add(executorId);

        final String executorString = String.join(",", existingExecutors);

        final ExecutorRepresenter remote = val.left();
        remote.sendControlMessage(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
          .setType(ControlMessage.MessageType.ExecutorRegistered)
          .setRegisteredExecutor(executorString)
          .build());

      });

      // 2. Second, send current registered executors for me
      final Set<String> existingExecutors =
        executors.values().stream().map(e -> e.left().getExecutorId())
          .collect(Collectors.toSet());

      if (existingExecutors.size() > 0) {
        final String executorString = String.join(",", existingExecutors);
        executor.sendControlMessage(ControlMessage.Message.newBuilder()
          .setId(RuntimeIdManager.generateMessageId())
          .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
          .setType(ControlMessage.MessageType.ExecutorRegistered)
          .setRegisteredExecutor(executorString)
          .build());
      }

      executors.put(executorId, Pair.of(executor, ExecutorState.RUNNING));
      if (executor.getExecutorId().contains("Lambda")) {
        lambdaExecutors.add(executorId);
      }
    }
  }

  // TODO: call this method
  synchronized void removeExecutor(final String executor) {
    if (!executors.containsKey(executor)) {
      throw new IllegalArgumentException("No executor: " + executor);
    }

    LOG.info("Remove executor {}", executor);

    final ExecutorRepresenter executorRepresenter = executors.remove(executor).left();
    lambdaExecutors.remove(executor);
    executorRepresenter.shutDown();

    // broadcast executors
    executors.values().forEach(val -> {
      final ExecutorRepresenter remote = val.left();
      remote.sendControlMessage(ControlMessage.Message.newBuilder()
        .setId(RuntimeIdManager.generateMessageId())
        .setListenerId(EXECUTOR_MESSAGE_LISTENER_ID.ordinal())
        .setType(ControlMessage.MessageType.ExecutorRemoved)
        .setRegisteredExecutor(executor)
        .build());
    });
  }

  public void viewLambdaExecutors(final Consumer<Set<ExecutorRepresenter>> consumer) {
    consumer.accept(getLambdaExecutors());
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

  public Set<ExecutorRepresenter> getLambdaExecutors() {
    return lambdaExecutors.stream()
      .map(lambdaExecutor -> executors.get(lambdaExecutor).left())
      .collect(Collectors.toSet());
  }

  public Set<ExecutorRepresenter> getRunningExecutors() {
    return executors.values()
        .stream()
        .filter(pair -> pair.right().equals(ExecutorState.RUNNING))
        .map(Pair::left)
        .collect(Collectors.toSet());
  }

  public ExecutorRepresenter getExecutorRepresentor(final String executorId) {
    LOG.info("Get executorRepresenter {}", executorId);
    return executors.get(executorId).left();
  }

  @Override
  public String toString() {
    return executors.toString();
  }
}
