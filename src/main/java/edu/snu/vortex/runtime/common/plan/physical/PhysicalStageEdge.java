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


import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionPropertyMap;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.executor.data.HashRange;

import java.util.HashMap;
import java.util.Map;

/**
 * Contains information stage boundary {@link edu.snu.vortex.runtime.common.plan.stage.StageEdge}.
 */
public final class PhysicalStageEdge extends RuntimeEdge<PhysicalStage> {
  /**
   * The source {@link IRVertex}.
   * This belongs to the srcStage.
   */
  private final IRVertex srcVertex;

  /**
   * The destination {@link IRVertex}.
   * This belongs to the dstStage.
   */
  private final IRVertex dstVertex;

  /**
   * The map between the task group id and hash range to read.
   */
  private final Map<String, HashRange> taskGroupIdToHashRangeMap;

  public PhysicalStageEdge(final String runtimeEdgeId,
                           final ExecutionPropertyMap edgeProperties,
                           final IRVertex srcVertex,
                           final IRVertex dstVertex,
                           final PhysicalStage srcStage,
                           final PhysicalStage dstStage,
                           final Coder coder) {
    super(runtimeEdgeId, edgeProperties, srcStage, dstStage, coder);
    this.srcVertex = srcVertex;
    this.dstVertex = dstVertex;
    this.taskGroupIdToHashRangeMap = new HashMap<>();
  }

  public IRVertex getSrcVertex() {
    return srcVertex;
  }

  public IRVertex getDstVertex() {
    return dstVertex;
  }

  @Override
  public String propertiesToJSON() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{\"runtimeEdgeId\": \"").append(getId());
    sb.append("\", \"edgeProperties\": ").append(getExecutionProperties());
    sb.append(", \"externalSrcVertexId\": \"").append(srcVertex.getId());
    sb.append("\", \"externalDstVertexId\": \"").append(dstVertex.getId());
    sb.append("\", \"coder\": \"").append(getCoder().toString());
    sb.append("\"}");
    return sb.toString();
  }

  public Map<String, HashRange> getTaskGroupIdToHashRangeMap() {
    return taskGroupIdToHashRangeMap;
  }
}
