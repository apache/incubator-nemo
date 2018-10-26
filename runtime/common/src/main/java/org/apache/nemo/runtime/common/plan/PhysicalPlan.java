/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.common.plan;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.vertex.IRVertex;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A job's physical plan, to be executed by the Runtime.
 */
public final class PhysicalPlan implements Serializable {
  private final String id;
  private final DAG<Stage, StageEdge> stageDAG;
  private final Map<String, IRVertex> idToIRVertex;

  /**
   * Constructor.
   *
   * @param id       the ID of the plan.
   * @param stageDAG the DAG of stages.
   */
  public PhysicalPlan(final String id,
                      final DAG<Stage, StageEdge> stageDAG) {
    this.id = id;
    this.stageDAG = stageDAG;

    idToIRVertex = new HashMap<>();
    for (final Stage stage : stageDAG.getVertices()) {
      for (final IRVertex irVertex : stage.getIRDAG().getVertices()) {
        idToIRVertex.put(irVertex.getId(), irVertex);
      }
    }
  }

  /**
   * @return the ID of the plan.
   */
  public String getPlanId() {
    return id;
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
