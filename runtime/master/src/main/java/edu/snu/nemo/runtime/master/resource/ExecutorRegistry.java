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

import javax.inject.Inject;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

@DriverSide
@ThreadSafe
public final class ExecutorRegistry {
  private final Map<String, ExecutorRepresenter> executorRepresenterMap = new ConcurrentHashMap<>();

  @Inject
  private ExecutorRegistry() {
  }

  /**
   * @param executorId the executor id
   * @return the corresponding {@link ExecutorRepresenter}
   * @throws NoSuchExecutorException when the executor was not found
   */
  public synchronized ExecutorRepresenter getRepresenter(final String executorId) throws NoSuchExecutorException {
    final ExecutorRepresenter representer = executorRepresenterMap.get(executorId);
    if (representer == null) {
      throw new NoSuchExecutorException(executorId);
    }
    return representer;
  }

  /**
   * Returns a {@link Set} of executor ids in the registry.
   * Note the set is not modifiable. Also, further changes in the registry will not be reflected to the set.
   * @return a {@link Set} of executor ids in the registry
   */
  public synchronized Set<String> getExecutorIds() {
    return Collections.unmodifiableSet(new TreeSet<>(executorRepresenterMap.keySet()));
  }

  /**
   * Adds executor representer.
   * @param representer the {@link ExecutorRepresenter} to register.
   * @throws DuplicateExecutorIdException on multiple attempts to register same representer,
   *         or different representers with same executor id.
   */
  public synchronized void registerRepresenter(final ExecutorRepresenter representer) throws DuplicateExecutorIdException {
    final String executorId = representer.getExecutorId();
    executorRepresenterMap.compute(executorId, (id, existingRepresenter) -> {
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
    if (executorRepresenterMap.remove(executorId) == null) {
      throw new NoSuchExecutorException(executorId);
    }
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
