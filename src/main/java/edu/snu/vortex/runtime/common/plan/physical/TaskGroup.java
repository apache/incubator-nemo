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

import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.common.dag.DAG;

import java.io.Serializable;

/**
 * A TaskGroup is a grouping of {@link Task} that belong to a stage.
 * Executors receive units of TaskGroups during job execution,
 * and thus the resource type of all tasks of a TaskGroup must be identical.
 * A stage contains a list of TaskGroups whose length corresponds to stage/operator parallelism.
 */
public final class TaskGroup implements Serializable {
  private final String taskGroupId;
  private final String stageId;
  private final int taskGroupIdx;
  private final DAG<Task, RuntimeEdge<Task>> taskDAG;
  private final Attribute containerType;

  public TaskGroup(final String taskGroupId,
                   final String stageId,
                   final int taskGroupIdx,
                   final DAG<Task, RuntimeEdge<Task>> taskDAG,
                   final Attribute containerType) {
    this.taskGroupId = taskGroupId;
    this.stageId = stageId;
    this.taskGroupIdx = taskGroupIdx;
    this.taskDAG = taskDAG;
    this.containerType = containerType;
  }

  public String getTaskGroupId() {
    return taskGroupId;
  }

  public String getStageId() {
    return stageId;
  }

  public int getTaskGroupIdx() {
    return taskGroupIdx;
  }

  public Attribute getContainerType() {
    return containerType;
  }

  public DAG<Task, RuntimeEdge<Task>> getTaskDAG() {
    return taskDAG;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"taskGroupId\": \"").append(taskGroupId).append("\", ");
    sb.append("\"taskGroupIdx\": ").append(taskGroupIdx).append(", ");
    sb.append("\"taskDAG\": ").append(taskDAG).append(", ");
    sb.append("\"containerType\": \"").append(containerType).append("\"}");
    return sb.toString();
  }
}
