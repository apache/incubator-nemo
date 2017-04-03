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
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.runtime.common.RuntimeAttribute;

import java.util.HashSet;
import java.util.Set;

/**
 * Contains information/state regarding an executor.
 * Such information may include:
 *    a) The executor's resource type.
 *    b) The executor's capacity (ex. number of cores).
 *    c) Task groups scheduled/launched for the executor.
 *    d) (Please add other information as we implement more features).
 */
public final class ExecutorRepresenter {

  private final String executorId;
  private final RuntimeAttribute resourceType;
  private final int executorCapacity;
  private final Set<String> scheduledTaskGroups;
  private final Set<String> runningTaskGroups;

  public ExecutorRepresenter(final String executorId,
                             final RuntimeAttribute resourceType,
                             final int executorCapacity) {
    this.executorId = executorId;
    this.resourceType = resourceType;
    this.executorCapacity = executorCapacity;
    this.scheduledTaskGroups = new HashSet<>();
    this.runningTaskGroups = new HashSet<>();
  }

  public void onTaskGroupScheduled(final String taskGroupId) {

  }

  public void onTaskGroupLaunched(final String taskGroupId) {

  }

  public void onTaskGroupCompleted(final String taskGroupId) {

  }

  public String getExecutorId() {
    return executorId;
  }

  public RuntimeAttribute getResourceType() {
    return resourceType;
  }
}

