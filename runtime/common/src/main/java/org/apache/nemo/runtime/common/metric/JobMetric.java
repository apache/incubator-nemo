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
package org.apache.nemo.runtime.common.metric;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.exception.MetricException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.plan.Stage;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.common.state.PlanState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Metric class for Job (or {@link PhysicalPlan}).
 */
public final class JobMetric implements StateMetric<PlanState.State> {
  private final String id;
  private final List<StateTransitionEvent<PlanState.State>> stateTransitionEvents;
  private String irDagSummary;
  private Long inputSize;
  private String vertexProperties;
  private String edgeProperties;
  private JsonNode irDagJson;
  private volatile DAG<Stage, StageEdge> stageDAG;
  private JsonNode stageDagJson;
  private Long jobDuration;

  /**
   * Constructor.
   *
   * @param physicalPlan physical plan to derive the id from.
   */
  public JobMetric(final PhysicalPlan physicalPlan) {
    this(physicalPlan.getPlanId());
  }

  /**
   * Constructor with the designated id.
   *
   * @param id the id.
   */
  public JobMetric(final String id) {
    this.id = id;
    this.stateTransitionEvents = new ArrayList<>();
  }

  @JsonProperty("ir-dag")
  public JsonNode getIRDAG() {
    return irDagJson;
  }

  public String getIrDagSummary() {
    return this.irDagSummary;
  }

  public Long getInputSize() {
    return this.inputSize;
  }

  public String getVertexProperties() {
    return this.vertexProperties;
  }

  public String getEdgeProperties() {
    return this.edgeProperties;
  }

  /**
   * Setter for the IR DAG.
   *
   * @param irDag the IR DAG.
   */
  public void setIRDAG(final IRDAG irDag) {
    this.irDagSummary = irDag.irDAGSummary();
    this.inputSize = irDag.getRootVertices().stream()
      .filter(irVertex -> irVertex instanceof SourceVertex)
      .mapToLong(srcVertex -> {
        try {
          return ((SourceVertex) srcVertex).getEstimatedSizeBytes();
        } catch (Exception e) {
          throw new MetricException(e);
        }
      })
      .sum();
    final Pair<String, String> stringifiedProperties = MetricUtils.stringifyIRDAGProperties(irDag);
    this.vertexProperties = stringifiedProperties.left();
    this.edgeProperties = stringifiedProperties.right();
    final ObjectMapper objectMapper = new ObjectMapper();
    try {
      this.irDagJson = objectMapper.readTree(irDag.toString());
    } catch (final IOException e) {
      throw new MetricException(e);
    }
  }

  @JsonProperty("stage-dag")
  public JsonNode getStageDAG() {
    return stageDagJson;
  }

  /**
   * Setter for the stage DAG.
   *
   * @param dag the stage DAG.
   */
  public void setStageDAG(final DAG<Stage, StageEdge> dag) {
    final String dagJson = dag.toString();
    final ObjectMapper objectMapper = new ObjectMapper();
    try {
      this.stageDagJson = objectMapper.readTree(dagJson);
    } catch (final IOException e) {
      throw new MetricException(e);
    }
  }

  public Long getJobDuration() {
    return jobDuration;
  }

  public void setJobDuration(final Long jobDuration) {
    this.jobDuration = jobDuration;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public List<StateTransitionEvent<PlanState.State>> getStateTransitionEvents() {
    return stateTransitionEvents;
  }

  @Override
  public void addEvent(final PlanState.State prevState, final PlanState.State newState) {
    stateTransitionEvents.add(new StateTransitionEvent<>(System.currentTimeMillis(), prevState, newState));
  }

  @Override
  public boolean processMetricMessage(final String metricField, final byte[] metricValue) {
    // do nothing
    return false;
  }
}
