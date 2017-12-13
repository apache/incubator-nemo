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
package edu.snu.onyx.runtime.common.plan.stage;

import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.dag.Vertex;

/**
 * Represents a stage in Runtime's execution of a job.
 * Each stage contains a part of a whole execution plan.
 * Stage partitioning is determined by {@link edu.snu.onyx.compiler.backend.onyx.OnyxBackend}.
 */
public final class Stage extends Vertex {
  private final DAG<IRVertex, IREdge> stageInternalDAG;
  private final int scheduleGroupIndex;

  /**
   * Constructor.
   * @param stageId id of the stage.
   * @param stageInternalDAG the internal DAG of the stage.
   * @param scheduleGroupIndex the schedule group index.
   */
  public Stage(final String stageId,
               final DAG<IRVertex, IREdge> stageInternalDAG,
               final int scheduleGroupIndex) {
    super(stageId);
    this.stageInternalDAG = stageInternalDAG;
    this.scheduleGroupIndex = scheduleGroupIndex;
  }

  /**
   * @return the internal DAG of the stage.
   */
  public DAG<IRVertex, IREdge> getStageInternalDAG() {
    return stageInternalDAG;
  }

  /**
   * @return the schedule group index.
   */
  public int getScheduleGroupIndex() {
    return scheduleGroupIndex;
  }

  @Override
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"scheduleGroupIndex\": ").append(scheduleGroupIndex);
    sb.append(", \"stageInternalDAG\": ").append(stageInternalDAG.toString());
    sb.append("}");
    return sb.toString();
  }
}
