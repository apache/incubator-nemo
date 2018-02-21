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
package edu.snu.nemo.runtime.master.resource;

import net.jcip.annotations.ThreadSafe;
import org.apache.reef.annotations.audience.DriverSide;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maintains map between executor id and {@link ExecutorRepresenter}.
 */
@DriverSide
@ThreadSafe
public final class ExecutorRegistry {
  private final Map<String, ExecutorRepresenter> runningExecutorRepresenterMap = new ConcurrentHashMap<>();
  private final Map<String, ExecutorRepresenter> failedExecutorRepresenterMap = new ConcurrentHashMap<>();

  @Inject
  private ExecutorRegistry() {
  }

  /**
   * @param executorId the executor id
   * @return the corresponding {@link ExecutorRepresenter} that has not failed
   * @throws NoSuchExecutorException when the executor was not found
   */
  @Nonnull
  public synchronized ExecutorRepresenter getExecutorRepresenter(final String executorId)
      throws NoSuchExecutorException {
    try {
      return getRunningExecutorRepresenter(executorId);
    } catch (final NoSuchExecutorException e) {
      return getFailedExecutorRepresenter(executorId);
    }
  }

  /**
   * @param executorId the executor id
   * @return the corresponding {@link ExecutorRepresenter} that has not failed
   * @throws NoSuchExecutorException when the executor was not found
   */
  @Nonnull
  public synchronized ExecutorRepresenter getRunningExecutorRepresenter(final String executorId)
      throws NoSuchExecutorException {
    final ExecutorRepresenter representer = runningExecutorRepresenterMap.get(executorId);
    if (representer == null) {
      throw new NoSuchExecutorException(executorId);
    }
    return representer;
  }

  /**
   * @param executorId the executor id
   * @return the corresponding {@link ExecutorRepresenter} that has not failed
   * @throws NoSuchExecutorException when the executor was not found
   */
  @Nonnull
  public synchronized ExecutorRepresenter getFailedExecutorRepresenter(final String executorId)
      throws NoSuchExecutorException {
    final ExecutorRepresenter representer = failedExecutorRepresenterMap.get(executorId);
    if (representer == null) {
      throw new NoSuchExecutorException(executorId);
    }
    return representer;
  }

  /**
   * Returns a {@link Set} of running executor ids in the registry.
   * Note the set is not modifiable. Also, further changes in the registry will not be reflected to the set.
   * @return a {@link Set} of executor ids for running executors in the registry
   */
  public synchronized Set<String> getRunningExecutorIds() {
    return Collections.unmodifiableSet(new TreeSet<>(runningExecutorRepresenterMap.keySet()));
  }

  /**
   * Returns a {@link Set} of failed executor ids in the registry.
   * Note the set is not modifiable. Also, further changes in the registry will not be reflected to the set.
   * @return a {@link Set} of failed executor ids
   */
  public synchronized Set<String> getFailedExecutorIds() {
    return Collections.unmodifiableSet(new TreeSet<>(failedExecutorRepresenterMap.keySet()));
  }

  /**
   * Adds executor representer.
   * @param representer the {@link ExecutorRepresenter} to register.
   * @throws DuplicateExecutorIdException on multiple attempts to register same representer,
   *         or different representers with same executor id.
   */
  public synchronized void registerRepresenter(final ExecutorRepresenter representer)
      throws DuplicateExecutorIdException {
    final String executorId = representer.getExecutorId();
    if (failedExecutorRepresenterMap.get(executorId) != null) {
      throw new DuplicateExecutorIdException(executorId);
    }
    runningExecutorRepresenterMap.compute(executorId, (id, existingRepresenter) -> {
      if (existingRepresenter != null) {
        throw new DuplicateExecutorIdException(id);
      }
      return representer;
    });
  }

  /**
   * Removes executor representer that has the specified executor id.
   * @param executorId the executor id
   * @throws NoSuchExecutorException when the specified executor id is not registered
   */
  public synchronized void deregisterRepresenter(final String executorId) throws NoSuchExecutorException {
    if (runningExecutorRepresenterMap.remove(executorId) != null) {
      return;
    }
    if (failedExecutorRepresenterMap.remove(executorId) != null) {
      return;
    }
    throw new NoSuchExecutorException(executorId);
  }

  /**
   * Moves the representer into the pool of representer of the failed executors.
   * @param executorId the corresponding executor id
   * @throws NoSuchExecutorException when the specified executor id is not registered, or already set as failed
   */
  public synchronized void setRepresenterAsFailed(final String executorId) throws NoSuchExecutorException {
    final ExecutorRepresenter representer = runningExecutorRepresenterMap.remove(executorId);
    if (representer == null) {
      throw new NoSuchExecutorException(executorId);
    }
    failedExecutorRepresenterMap.put(executorId, representer);
  }

  /**
   * Exception that indicates multiple attempts to register executors with same executor id.
   */
  public final class DuplicateExecutorIdException extends RuntimeException {
    private final String executorId;

    /**
     * @param executorId the executor id that caused this exception
     */
    public DuplicateExecutorIdException(final String executorId) {
      super(String.format("Duplicate executorId: %s", executorId));
      this.executorId = executorId;
    }

    /**
     * @return the executor id for this exception
     */
    public String getExecutorId() {
      return executorId;
    }
  }

  /**
   * Exception that indicates no executor for the specified executorId.
   */
  public final class NoSuchExecutorException extends RuntimeException {
    private final String executorId;

    /**
     * @param executorId the executor id that caused this exception
     */
    public NoSuchExecutorException(final String executorId) {
      super(String.format("No such executor: %s", executorId));
      this.executorId = executorId;
    }

    /**
     * @return the executor id for this exception
     */
    public String getExecutorId() {
      return executorId;
    }
  }
}
