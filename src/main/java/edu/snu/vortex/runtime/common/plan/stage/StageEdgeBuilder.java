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
package edu.snu.vortex.runtime.common.plan.stage;


import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.attribute.AttributeMap;

/**
 * Stage Edge Builder.
 */
public final class StageEdgeBuilder {
  private final String stageEdgeId;
  private AttributeMap edgeAttributes;
  private Stage srcStage;
  private Stage dstStage;
  private IRVertex srcVertex;
  private IRVertex dstVertex;
  private Coder coder;

  /**
   * Represents the edge between vertices in a logical plan.
   * @param irEdgeId id of this edge.
   */
  public StageEdgeBuilder(final String irEdgeId) {
    this.stageEdgeId = irEdgeId;
  }

  public StageEdgeBuilder setEdgeAttributes(final AttributeMap ea) {
    this.edgeAttributes = ea;
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

  public StageEdge build() {
    return new StageEdge(stageEdgeId,
        edgeAttributes, srcStage, dstStage, coder, srcVertex, dstVertex);
  }
}
