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
package edu.snu.vortex.runtime.common.plan.physical;

import edu.snu.vortex.common.dag.Vertex;

/**
 * Task.
 * The index value is identical to the TaskGroup's index it belongs to.
 */
public abstract class Task extends Vertex {
  private final String runtimeVertexId;
  private final int index;

  public Task(final String taskId,
              final String runtimeVertexId,
              final int index) {
    super(taskId);
    this.runtimeVertexId = runtimeVertexId;
    this.index = index;
  }

  public final String getRuntimeVertexId() {
    return runtimeVertexId;
  }

  public final int getIndex() {
    return index;
  }

  @Override
  public final String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"runtimeVertexId\": \"").append(runtimeVertexId).append("\", ");
    sb.append("\"index\": ").append(index).append("}");
    return sb.toString();
  }
}
