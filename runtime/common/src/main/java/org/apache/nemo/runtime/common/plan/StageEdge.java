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
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.nemo.common.HashRange;
import org.apache.nemo.common.KeyRange;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty;
import org.apache.nemo.common.ir.edge.executionproperty.PartitionSetProperty;
import org.apache.nemo.common.ir.edge.executionproperty.SubPartitionSetProperty;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Edge of a stage that connects an IRVertex of the source stage to an IRVertex of the destination stage.
 * This means that there can be multiple StageEdges between two Stages.
 */
public final class StageEdge extends RuntimeEdge<Stage> {
  private static final Logger LOG = LoggerFactory.getLogger(StageEdge.class.getName());

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
    return getExecutionProperties().get(CommunicationPatternProperty.class)
      .orElseThrow(() -> new RuntimeException(String.format(
        "CommunicationPatternProperty not set for %s", getId())));
  }

  /**
   * @return {@link DataFlowProperty} value.
   */
  public DataFlowProperty.Value getDataFlowModel() {
    return getExecutionProperties().get(DataFlowProperty.class)
      .orElseThrow(() -> new RuntimeException(String.format(
        "DataFlowProperty not set for %s", getId())));
  }

  /**
   * Get keyRanges for shuffle edge.
   * If the destination vertex is enabled for dynamic task sizing,
   * @return {@link org.apache.nemo.common.ir.edge.executionproperty.SubPartitionSetProperty} value.
   * Else,
   * @return {@link org.apache.nemo.common.ir.edge.executionproperty.PartitionSetProperty} value.
   * If both doesn't exist, return default partition set made from parallelism.
   */
  public List<KeyRange> getKeyRanges() {
    final ArrayList<KeyRange> defaultPartitionSet = new ArrayList<>();
    final List<KeyRange> keyRanges;
    for (int taskIndex = 0; taskIndex < getDst().getParallelism(); taskIndex++) {
      defaultPartitionSet.add(taskIndex, HashRange.of(taskIndex, taskIndex + 1));
    }
    if (getDst().getEnableDynamicTaskSizing()) {
      keyRanges = getExecutionProperties()
        .get(SubPartitionSetProperty.class).orElse(defaultPartitionSet);
    } else {
      keyRanges = getExecutionProperties()
        .get(PartitionSetProperty.class).orElse(defaultPartitionSet);
    }
    return keyRanges;
  }
}
