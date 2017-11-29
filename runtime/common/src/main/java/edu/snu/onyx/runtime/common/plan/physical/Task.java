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
package edu.snu.onyx.runtime.common.plan.physical;

import edu.snu.onyx.common.dag.Vertex;

/**
 * Task.
 * The index value is identical to the TaskGroup's index it belongs to.
 */
public abstract class Task extends Vertex {
  private final String runtimeVertexId;
  private final int index;
  private final String taskGroupId;

  public Task(final String taskId,
              final String runtimeVertexId,
              final int index,
              final String taskGroupId) {
    super(taskId);
    this.runtimeVertexId = runtimeVertexId;
    this.index = index;
    this.taskGroupId = taskGroupId;
  }

  public final String getRuntimeVertexId() {
    return runtimeVertexId;
  }

  public final int getIndex() {
    return index;
  }

  public final String getTaskGroupId() {
    return taskGroupId;
  }

  @Override
  public final String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"runtimeVertexId\": \"").append(runtimeVertexId).append("\", ");
    sb.append("\"index\": ").append(index).append("}");
    return sb.toString();
  }
}
