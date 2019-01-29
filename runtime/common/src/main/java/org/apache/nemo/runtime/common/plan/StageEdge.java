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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import org.apache.nemo.common.HashRange;
import org.apache.nemo.common.KeyRange;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import org.apache.nemo.common.ir.edge.executionproperty.PartitionSetProperty;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Edge of a stage that connects an IRVertex of the source stage to an IRVertex of the destination stage.
 * This means that there can be multiple StageEdges between two Stages.
 */
public final class StageEdge extends RuntimeEdge<Stage> {
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

  // Key IRVertex properties.
  private final CommunicationPatternProperty.Value dataCommunicationPatternValue;
  private final DataFlowProperty.Value dataFlowModelValue;

  // Key ranges to read.
  private final List<KeyRange> keyRanges;


  /**
   * Constructor.
   *
   * @param runtimeEdgeId  id of the runtime edge.
   * @param edgeProperties edge execution properties.
   * @param srcVertex      source IRVertex in the srcStage of this edge.
   * @param dstVertex      destination IRVertex in the dstStage of this edge.
   * @param srcStage       source stage.
   * @param dstStage       destination stage.
   */
  @VisibleForTesting
  public StageEdge(final String runtimeEdgeId,
                   final ExecutionPropertyMap<EdgeExecutionProperty> edgeProperties,
                   final IRVertex srcVertex,
                   final IRVertex dstVertex,
                   final Stage srcStage,
                   final Stage dstStage) {
    super(runtimeEdgeId, edgeProperties, srcStage, dstStage);
    this.srcVertex = srcVertex;
    this.dstVertex = dstVertex;

    this.dataCommunicationPatternValue = edgeProperties.get(CommunicationPatternProperty.class)
      .orElseThrow(() -> new RuntimeException(String.format(
        "CommunicationPatternProperty not set for %s", runtimeEdgeId)));
    this.dataFlowModelValue = edgeProperties.get(DataFlowProperty.class)
      .orElseThrow(() -> new RuntimeException(String.format(
        "DataFlowProperty not set for %s", runtimeEdgeId)));
    // if not exists...
    final ArrayList<KeyRange> defaultPartitionSet = new ArrayList<>(dstStage.getParallelism());
    for (int taskIdx = 0; taskIdx < dstStage.getParallelism(); taskIdx++) {
      defaultPartitionSet.add(taskIdx, HashRange.of(taskIdx, taskIdx + 1));
    }
    this.keyRanges = edgeProperties.get(PartitionSetProperty.class).orElse(defaultPartitionSet);
  }

  /**
   * @return the source IR vertex of the edge.
   */
  public IRVertex getSrcIRVertex() {
    return srcVertex;
  }

  /**
   * @return the destination IR vertex of the edge.
   */
  public IRVertex getDstIRVertex() {
    return dstVertex;
  }

  @Override
    public ObjectNode getPropertiesAsJsonNode() {
    final ObjectNode node = JsonNodeFactory.instance.objectNode();
    node.put("runtimeEdgeId", getId());
    node.set("executionProperties", getExecutionProperties().asJsonNode());
    node.put("externalSrcVertexId", srcVertex.getId());
    node.put("externalDstVertexId", dstVertex.getId());
    return node;
  }

  @Override
  public String toString() {
    return getPropertiesAsJsonNode().toString();
  }

  /**
   * @param edge edge to compare.
   * @return whether or not the edge has the same itinerary
   */
  public Boolean hasSameItineraryAs(final StageEdge edge) {
    return getSrc().equals(edge.getSrc()) && getDst().equals(edge.getDst());
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final StageEdge stageEdge = (StageEdge) o;
    return getExecutionProperties().equals(stageEdge.getExecutionProperties()) && hasSameItineraryAs(stageEdge);
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
      .append(getSrc().hashCode())
      .append(getDst().hashCode())
      .append(getExecutionProperties())
      .toHashCode();
  }

  /**
   * @return {@link CommunicationPatternProperty} value.
   */
  public CommunicationPatternProperty.Value getDataCommunicationPattern() {
    return dataCommunicationPatternValue;
  }

  /**
   * @return {@link DataFlowProperty} value.
   */
  public DataFlowProperty.Value getDataFlowModel() {
    return dataFlowModelValue;
  }

  /**
   * @return {@link org.apache.nemo.common.ir.edge.executionproperty.PartitionSetProperty} value.
   */
  public List<KeyRange> getKeyRanges() {
    return keyRanges;
  }
}
