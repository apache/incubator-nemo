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


import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.ir.executionproperty.ExecutionPropertyMap;

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
  private Coder coder;
  private Boolean isSideInput;

  /**
   * Represents the edge between vertices in a logical plan.
   * @param irEdgeId id of this edge.
   */
  public StageEdgeBuilder(final String irEdgeId) {
    this.stageEdgeId = irEdgeId;
  }

  public StageEdgeBuilder setEdgeProperties(final ExecutionPropertyMap ea) {
    this.edgeProperties = ea;
    return this;
  }

  public StageEdgeBuilder setSrcStage(final Stage ss) {
    this.srcStage = ss;
    return this;
  }

  public StageEdgeBuilder setDstStage(final Stage ds) {
    this.dstStage = ds;
    return this;
  }

  public StageEdgeBuilder setSrcVertex(final IRVertex sv) {
    this.srcVertex = sv;
    return this;
  }

  public StageEdgeBuilder setDstVertex(final IRVertex dv) {
    this.dstVertex = dv;
    return this;
  }

  public StageEdgeBuilder setCoder(final Coder c) {
    this.coder = c;
    return this;
  }

  public StageEdgeBuilder setSideInputFlag(final Boolean sideInputFlag) {
    this.isSideInput = sideInputFlag;
    return this;
  }

  public StageEdge build() {
    return new StageEdge(stageEdgeId,
        edgeProperties, srcStage, dstStage, coder, isSideInput, srcVertex, dstVertex);
  }
}
