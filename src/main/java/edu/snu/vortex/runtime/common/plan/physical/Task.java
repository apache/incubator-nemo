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

import java.io.Serializable;

/**
 * Task.
 */
public abstract class Task implements Serializable {
  private final String taskId;
  private final String runtimeVertexId;
  private final int index;

  public Task(final String taskId,
              final String runtimeVertexId,
              final int index) {
    this.taskId = taskId;
    this.runtimeVertexId = runtimeVertexId;
    this.index = index;
  }

  public final String getTaskId() {
    return taskId;
  }

  public final String getRuntimeVertexId() {
    return runtimeVertexId;
  }

  public final int getIndex() {
    return index;
  }
}
