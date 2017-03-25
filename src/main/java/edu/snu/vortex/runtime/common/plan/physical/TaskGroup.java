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

import edu.snu.vortex.utils.DAG;

import java.io.Serializable;
import java.util.List;

/**
 * TaskGroup.
 */
public final class TaskGroup implements Serializable {
  private final String taskGroupId;
  private final DAG<Task> taskDAG;

  /**
   * List of information on incoming edges to the stage.
   */
  private final List<StageBoundaryEdgeInfo> incomingEdges;

  /**
   * List of information on outgoing edges from the stage.
   */
  private final List<StageBoundaryEdgeInfo> outgoingEdges;

  public TaskGroup(final String taskGroupId,
                   final DAG<Task> taskDAG,
                   final List<StageBoundaryEdgeInfo> incomingEdges,
                   final List<StageBoundaryEdgeInfo> outgoingEdges) {
    this.taskGroupId = taskGroupId;
    this.taskDAG = taskDAG;
    this.incomingEdges = incomingEdges;
    this.outgoingEdges = outgoingEdges;
  }
}
