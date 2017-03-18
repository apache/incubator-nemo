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
package edu.snu.vortex.runtime.common.execplan;

import edu.snu.vortex.runtime.common.*;
import edu.snu.vortex.runtime.common.RuntimeAttributes;
import edu.snu.vortex.runtime.common.task.Task;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Represents an operator of a job, tagged with attributes about the operator.
 */
public abstract class RuntimeVertex implements Serializable {
  private final String runtimeVertexId;
  private final Map<RuntimeAttributes.RuntimeVertexAttribute, Object> vertexAttributes;

  public RuntimeVertex(final String irVertexId,
                       final Map<RuntimeAttributes.RuntimeVertexAttribute, Object> vertexAttributes) {
    this.runtimeVertexId = RuntimeIdGenerator.generateRuntimeVertexId(irVertexId);
    this.vertexAttributes = vertexAttributes;
  }

  public final String getId() {
    return runtimeVertexId;
  }

  public abstract List<? extends Task> getTaskList();
}
