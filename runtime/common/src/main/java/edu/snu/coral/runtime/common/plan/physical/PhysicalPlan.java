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
package edu.snu.coral.runtime.common.plan.physical;

import edu.snu.coral.common.dag.DAG;
import edu.snu.coral.common.ir.vertex.IRVertex;

import java.io.Serializable;
import java.util.Map;

/**
 * A job's physical plan, to be executed by the Runtime.
 */
public final class PhysicalPlan implements Serializable {

  private final String id;

  private final DAG<PhysicalStage, PhysicalStageEdge> stageDAG;

  private final Map<Task, IRVertex> taskIRVertexMap;

  /**
   * Constructor.
   *
   * @param id              ID of the plan.
   * @param stageDAG        the DAG of stages.
   * @param taskIRVertexMap map from task to IR vertex.
   */
  public PhysicalPlan(final String id,
                      final DAG<PhysicalStage, PhysicalStageEdge> stageDAG,
                      final Map<Task, IRVertex> taskIRVertexMap) {
    this.id = id;
    this.stageDAG = stageDAG;
    this.taskIRVertexMap = taskIRVertexMap;
  }

  /**
   * @return id of the plan.
   */
  public String getId() {
    return id;
  }

  /**
   * @return the DAG of stages.
   */
  public DAG<PhysicalStage, PhysicalStageEdge> getStageDAG() {
    return stageDAG;
  }

  /**
   * Get an IR vertex of the given task.
   *
   * @param task task to find the IR vertex of.
   * @return the corresponding IR vertex of the given task.
   */
  public IRVertex getIRVertexOf(final Task task) {
    return taskIRVertexMap.get(task);
  }

  /**
   * @return the map from task to IR vertex.
   */
  public Map<Task, IRVertex> getTaskIRVertexMap() {
    return taskIRVertexMap;
  }

  @Override
  public String toString() {
    return stageDAG.toString();
  }
}
