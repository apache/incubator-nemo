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
package edu.snu.vortex.runtime.common.plan.logical;

import edu.snu.vortex.compiler.frontend.beam.BoundedSourceVertex;
import edu.snu.vortex.runtime.common.RuntimeAttributeMap;
import edu.snu.vortex.runtime.common.plan.physical.BoundedSourceTask;
import edu.snu.vortex.runtime.common.plan.physical.Task;
import java.util.ArrayList;
import java.util.List;
/**
 * Represents a source vertex for batch execution.
 */
public final class RuntimeBoundedSourceVertex extends RuntimeVertex {
  private final List<BoundedSourceTask> taskList;
  private final BoundedSourceVertex boundedSourceVertex;

  public RuntimeBoundedSourceVertex(final BoundedSourceVertex boundedSourceVertex,
                                    final RuntimeAttributeMap vertexAttributes) {
    super(boundedSourceVertex.getId(), vertexAttributes);
    this.boundedSourceVertex = boundedSourceVertex;
    this.taskList = new ArrayList<>();
  }

  @Override
  public List<BoundedSourceTask> getTaskList() {
    return taskList;
  }

  @Override
  public void addTask(final Task task) {
    taskList.add((BoundedSourceTask) task);
  }

  public BoundedSourceVertex getBoundedSourceVertex() {
    return boundedSourceVertex;
  }

  @Override
  public String toString() {
    return boundedSourceVertex.toString();
  }
}
