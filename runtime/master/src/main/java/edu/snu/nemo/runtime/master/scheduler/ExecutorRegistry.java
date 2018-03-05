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

import edu.snu.nemo.runtime.master.resource.ExecutorRepresenter;
import org.apache.reef.annotations.audience.DriverSide;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.*;

/**
 * (WARNING) This class is not thread-safe.
 * (i.e., Only a SchedulingPolicy accesses this class)
 *
 * Maintains map between executor id and {@link ExecutorRepresenter}.
 */
@DriverSide
@NotThreadSafe
public final class ExecutorRegistry {
  private final Map<String, ExecutorRepresenter> runningExecutors;
  private final Map<String, ExecutorRepresenter> failedExecutors;
  private final Map<String, ExecutorRepresenter> completedExecutors;

  @Inject
  public ExecutorRegistry() {
    this.runningExecutors = new HashMap<>();
    this.failedExecutors = new HashMap<>();
    this.completedExecutors = new HashMap<>();
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer();
    sb.append("Running: ");
    sb.append(runningExecutors.toString());
    sb.append("/ Failed: ");
    sb.append(failedExecutors.toString());
    sb.append("/ Completed: ");
    sb.append(completedExecutors.toString());
    return sb.toString();
  }

  /**
   * @param executorId the executor id
   * @return the corresponding {@link ExecutorRepresenter} that has not failed
   * @throws NoSuchExecutorException when the executor was not found
   */
  @Nonnull
  public ExecutorRepresenter getExecutorRepresenter(final String executorId) throws NoSuchExecutorException {
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
  public ExecutorRepresenter getRunningExecutorRepresenter(final String executorId) throws NoSuchExecutorException {
    final ExecutorRepresenter representer = runningExecutors.get(executorId);
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
  public ExecutorRepresenter getFailedExecutorRepresenter(final String executorId) throws NoSuchExecutorException {
    final ExecutorRepresenter representer = failedExecutors.get(executorId);
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
  public Set<String> getRunningExecutorIds() {
    return Collections.unmodifiableSet(new TreeSet<>(runningExecutors.keySet()));
  }

  /**
   * Adds executor representer.
   * @param representer the {@link ExecutorRepresenter} to register.
   * @throws DuplicateExecutorIdException on multiple attempts to register same representer,
   *         or different representers with same executor id.
   */
  public void registerRepresenter(final ExecutorRepresenter representer) throws DuplicateExecutorIdException {
    final String executorId = representer.getExecutorId();
    if (failedExecutors.get(executorId) != null) {
      throw new DuplicateExecutorIdException(executorId);
    }
    runningExecutors.compute(executorId, (id, existingRepresenter) -> {
      if (existingRepresenter != null) {
        throw new DuplicateExecutorIdException(id);
      }
      return representer;
    });
  }

  /**
   * Moves the representer into the pool of representer of the failed executors.
   * @param executorId the corresponding executor id
   * @throws NoSuchExecutorException when the specified executor id is not registered, or already set as failed
   */
  public void setRepresenterAsFailed(final String executorId) throws NoSuchExecutorException {
    final ExecutorRepresenter representer = runningExecutors.remove(executorId);
    if (representer == null) {
      throw new NoSuchExecutorException(executorId);
    }
    failedExecutors.put(executorId, representer);
  }

  /**
   * Moves the representer into the pool of representer of the failed executors.
   * @param executorId the corresponding executor id
   * @throws NoSuchExecutorException when the specified executor id is not registered, or already set as failed
   */
  public void setRepresenterAsCompleted(final String executorId) throws NoSuchExecutorException {
    final ExecutorRepresenter representer = runningExecutors.remove(executorId);
    if (representer == null) {
      throw new NoSuchExecutorException(executorId);
    }
    if (failedExecutors.containsKey(executorId)) {
      throw new IllegalStateException(executorId + " is in " + failedExecutors);
    }
    if (completedExecutors.containsKey(executorId)) {
      throw new IllegalStateException(executorId + " is already in " + completedExecutors);
    }

    completedExecutors.put(executorId, representer);
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
