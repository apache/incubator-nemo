/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.nemo.runtime.common.plan;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.vertex.IRVertex;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A job's physical plan, to be executed by the Runtime.
 */
public final class PhysicalPlan implements Serializable {
  private final String jobId;
  private final String planId;
  private final DAG<Stage, StageEdge> stageDAG;
  private final Map<String, IRVertex> idToIRVertex;

  /**
   * Constructor.
   *
   * @param jobId    the job ID.
   * @param planId   the ID of the plan.
   * @param stageDAG the DAG of stages.
   */
  public PhysicalPlan(final String jobId,
                      final String planId,
                      final DAG<Stage, StageEdge> stageDAG) {
    this.jobId = jobId;
    this.planId = planId;
    this.stageDAG = stageDAG;

    idToIRVertex = new HashMap<>();
    for (final Stage stage : stageDAG.getVertices()) {
      for (final IRVertex irVertex : stage.getIRDAG().getVertices()) {
        idToIRVertex.put(irVertex.getId(), irVertex);
      }
    }
  }

  /**
   * @return the job ID.
   */
  public String getJobId() {
    return jobId;
  }

  /**
   * @return the ID of the plan.
   */
  public String getPlanId() {
    return planId;
  }

  /**
   * @return the DAG of stages.
   */
  public DAG<Stage, StageEdge> getStageDAG() {
    return stageDAG;
  }

  /**
   * @return the map from task to IR vertex.
   */
  public Map<String, IRVertex> getIdToIRVertex() {
    return idToIRVertex;
  }

  @Override
  public String toString() {
    return stageDAG.toString();
  }
}
