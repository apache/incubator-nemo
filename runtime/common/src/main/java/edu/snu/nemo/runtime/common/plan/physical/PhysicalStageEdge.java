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

import edu.snu.nemo.common.coder.Coder;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import edu.snu.nemo.runtime.common.data.KeyRange;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;
import edu.snu.nemo.runtime.common.data.HashRange;

import java.util.ArrayList;
import java.util.List;

/**
 * Contains information stage boundary {@link edu.snu.nemo.runtime.common.plan.stage.StageEdge}.
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
   * The list between the task group idx and key range to read.
   */
  private List<KeyRange> taskGroupIdxToKeyRange;

  /**
   * Constructor.
   * @param runtimeEdgeId id of the runtime edge.
   * @param edgeProperties edge execution properties.
   * @param srcVertex source vertex.
   * @param dstVertex destination vertex.
   * @param srcStage source stage.
   * @param dstStage destination stage.
   * @param coder the coder for enconding and deconding.
   * @param isSideInput whether or not the edge is a sideInput edge.
   */
  public PhysicalStageEdge(final String runtimeEdgeId,
                           final ExecutionPropertyMap edgeProperties,
                           final IRVertex srcVertex,
                           final IRVertex dstVertex,
                           final PhysicalStage srcStage,
                           final PhysicalStage dstStage,
                           final Coder coder,
                           final Boolean isSideInput) {
    super(runtimeEdgeId, edgeProperties, srcStage, dstStage, coder, isSideInput);
    this.srcVertex = srcVertex;
    this.dstVertex = dstVertex;
    // Initialize the key range of each dst task.
    this.taskGroupIdxToKeyRange = new ArrayList<>();
    for (int taskIdx = 0; taskIdx < dstStage.getTaskGroupIds().size(); taskIdx++) {
      taskGroupIdxToKeyRange.add(HashRange.of(taskIdx, taskIdx + 1));
    }
  }

  /**
   * @return the source vertex of the edge.
   */
  public IRVertex getSrcVertex() {
    return srcVertex;
  }

  /**
   * @return the destination vertex of the edge.
   */
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

  /**
   * @return the list between the task group idx and key range to read.
   */
  public List<KeyRange> getTaskGroupIdxToKeyRange() {
    return taskGroupIdxToKeyRange;
  }

  /**
   * Sets the task group idx to key range list.
   * @param taskGroupIdxToKeyRange the list to set.
   */
  public void setTaskGroupIdxToKeyRange(final List<KeyRange> taskGroupIdxToKeyRange) {
    this.taskGroupIdxToKeyRange = taskGroupIdxToKeyRange;
  }
}
