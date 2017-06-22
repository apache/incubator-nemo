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

import edu.snu.vortex.common.dag.DAG;

import java.util.logging.Logger;

/**
 * Represents a job.
 * Each execution plan consists of a list of {@link Stage} to execute, in a topological order.
 * An execution plan is submitted to {@link edu.snu.vortex.runtime.master.RuntimeMaster} once created.
 */
public final class ExecutionPlan {
  private static final Logger LOG = Logger.getLogger(ExecutionPlan.class.getName());

  private final String id;

  private final DAG<Stage, StageEdge> runtimeStageDAG;

  public ExecutionPlan(final String id,
                       final DAG<Stage, StageEdge> runtimeStageDAG) {
    this.id = id;
    this.runtimeStageDAG = runtimeStageDAG;
  }

  public String getId() {
    return id;
  }

  public DAG<Stage, StageEdge> getRuntimeStageDAG() {
    return runtimeStageDAG;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("ExecutionPlan{");
    sb.append("id='").append(id).append('\'');
    sb.append(", runtimeStageDAG=").append(runtimeStageDAG);
    sb.append('}');
    return sb.toString();
  }
}
