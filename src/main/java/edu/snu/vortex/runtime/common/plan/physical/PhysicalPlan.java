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

import edu.snu.vortex.utils.dag.DAG;

import java.util.logging.Logger;

/**
 * A job's physical plan corresponding to an {@link edu.snu.vortex.runtime.common.plan.logical.ExecutionPlan}.
 */
public final class PhysicalPlan {
  private static final Logger LOG = Logger.getLogger(PhysicalPlan.class.getName());

  private final String id;

  private final DAG<PhysicalStage, PhysicalStageEdge> stageDAG;

  public PhysicalPlan(final String id,
                      final DAG<PhysicalStage, PhysicalStageEdge> stageDAG) {
    this.id = id;
    this.stageDAG = stageDAG;
  }

  public String getId() {
    return id;
  }

  public DAG<PhysicalStage, PhysicalStageEdge> getStageDAG() {
    return stageDAG;
  }

  @Override
  public String toString() {
    return stageDAG.toString();
  }
}
