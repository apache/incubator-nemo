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
package edu.snu.nemo.runtime.common.plan.physical;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.vertex.IRVertex;

import java.io.Serializable;
import java.util.Map;

/**
 * A job's physical plan, to be executed by the Runtime.
 */
public final class PhysicalPlan implements Serializable {
  private final String id;
  private final DAG<PhysicalStage, PhysicalStageEdge> stageDAG;
  private final Map<String, IRVertex> idToIRVertex;

  /**
   * Constructor.
   *
   * @param id              ID of the plan.
   * @param stageDAG        the DAG of stages.
   * @param idToIRVertex map from task to IR vertex.
   */
  public PhysicalPlan(final String id,
                      final DAG<PhysicalStage, PhysicalStageEdge> stageDAG,
                      final Map<String, IRVertex> idToIRVertex) {
    this.id = id;
    this.stageDAG = stageDAG;
    this.idToIRVertex = idToIRVertex;
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
