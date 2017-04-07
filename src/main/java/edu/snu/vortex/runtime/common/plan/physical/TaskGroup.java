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

import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.utils.DAG;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * TaskGroup.
 */
public final class TaskGroup implements Serializable {
  private final String taskGroupId;
  private final DAG<Task> taskDAG;
  private final RuntimeAttribute resourceType;

  /**
   * Map of this stage's vertex ID to the information on the set of incoming edges to the stage.
   */
  private final Map<String, Set<StageBoundaryEdgeInfo>> incomingEdges;

  /**
   * Map of this stage's vertex ID to the information on the set of outgoing edges from the stage.
   */
  private final Map<String, Set<StageBoundaryEdgeInfo>> outgoingEdges;

  public TaskGroup(final String taskGroupId,
                   final DAG<Task> taskDAG,
                   final RuntimeAttribute resourceType,
                   final Map<String, Set<StageBoundaryEdgeInfo>> incomingEdges,
                   final Map<String, Set<StageBoundaryEdgeInfo>> outgoingEdges) {
    this.taskGroupId = taskGroupId;
    this.taskDAG = taskDAG;
    this.resourceType = resourceType;
    this.incomingEdges = incomingEdges;
    this.outgoingEdges = outgoingEdges;
  }

  public RuntimeAttribute getResourceType() {
    return resourceType;
  }

  public DAG<Task> getTaskDAG() {
    return taskDAG;
  }

  public Map<String, Set<StageBoundaryEdgeInfo>> getIncomingEdges() {
    return incomingEdges;
  }

  public Map<String, Set<StageBoundaryEdgeInfo>> getOutgoingEdges() {
    return outgoingEdges;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("TaskGroup{");
    sb.append("taskGroupId='").append(taskGroupId).append('\'');
    sb.append(", taskDAG=").append(taskDAG);
    sb.append(", resourceType=").append(resourceType);
    sb.append(", incomingEdges=").append(incomingEdges);
    sb.append(", outgoingEdges=").append(outgoingEdges);
    sb.append('}');
    return sb.toString();
  }
}
