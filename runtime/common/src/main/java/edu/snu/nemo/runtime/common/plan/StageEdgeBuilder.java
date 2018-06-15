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

import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.executionproperty.ExecutionPropertyMap;

/**
 * Stage Edge Builder.
 */
public final class StageEdgeBuilder {
  private final String stageEdgeId;
  private ExecutionPropertyMap edgeProperties;
  private Stage srcStage;
  private Stage dstStage;
  private IRVertex srcVertex;
  private IRVertex dstVertex;
  private Boolean isSideInput;

  /**
   * Represents the edge between vertices in a logical plan.
   *
   * @param irEdgeId id of this edge.
   */
  public StageEdgeBuilder(final String irEdgeId) {
    this.stageEdgeId = irEdgeId;
  }

  /**
   * Setter for edge properties.
   *
   * @param ea the edge properties.
   * @return the updated StageEdgeBuilder.
   */
  public StageEdgeBuilder setEdgeProperties(final ExecutionPropertyMap ea) {
    this.edgeProperties = ea;
    return this;
  }

  /**
   * Setter for the source stage.
   *
   * @param ss the source stage.
   * @return the updated StageEdgeBuilder.
   */
  public StageEdgeBuilder setSrcStage(final Stage ss) {
    this.srcStage = ss;
    return this;
  }

  /**
   * Setter for the destination stage.
   *
   * @param ds the destination stage.
   * @return the updated StageEdgeBuilder.
   */
  public StageEdgeBuilder setDstStage(final Stage ds) {
    this.dstStage = ds;
    return this;
  }

  /**
   * Setter for the source vertex.
   *
   * @param sv the source vertex.
   * @return the updated StageEdgeBuilder.
   */
  public StageEdgeBuilder setSrcVertex(final IRVertex sv) {
    this.srcVertex = sv;
    return this;
  }

  /**
   * Setter for the destination vertex.
   *
   * @param dv the destination vertex.
   * @return the updated StageEdgeBuilder.
   */
  public StageEdgeBuilder setDstVertex(final IRVertex dv) {
    this.dstVertex = dv;
    return this;
  }

  /**
   * Setter for side input flag.
   *
   * @param sideInputFlag the side input flag.
   * @return the updated StageEdgeBuilder.
   */
  public StageEdgeBuilder setSideInputFlag(final Boolean sideInputFlag) {
    this.isSideInput = sideInputFlag;
    return this;
  }

  /**
   * @return the built stage edge.
   */
  public StageEdge build() {
    return new StageEdge(stageEdgeId, edgeProperties, srcVertex, dstVertex, srcStage, dstStage, isSideInput);
  }
}
