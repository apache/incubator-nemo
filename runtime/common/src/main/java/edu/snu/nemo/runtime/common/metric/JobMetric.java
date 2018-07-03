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
package edu.snu.nemo.runtime.common.metric;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.runtime.common.plan.PhysicalPlan;
import edu.snu.nemo.runtime.common.state.JobState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Metric class for Job (or {@link PhysicalPlan}).
 */
public final class JobMetric implements StateMetric<JobState.State> {
  private String id;
  private List<StateTransitionEvent<JobState.State>> stateTransitionEvents = new ArrayList<>();
  private JsonNode stageDagJson;

  public JobMetric(final PhysicalPlan physicalPlan) {
    this.id = physicalPlan.getId();
  }

  public JobMetric(final String id) {
    this.id = id;
  }

  @JsonProperty("dag")
  public JsonNode getStageDAG() {
    return stageDagJson;
  }

  public void setStageDAG(final DAG dag) {
    final String dagJson = dag.toString();
    final ObjectMapper objectMapper = new ObjectMapper();
    try {
      this.stageDagJson = objectMapper.readTree(dagJson);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public List<StateTransitionEvent<JobState.State>> getStateTransitionEvents() {
    return stateTransitionEvents;
  }

  @Override
  public void addEvent(final JobState.State prevState, final JobState.State newState) {
    stateTransitionEvents.add(new StateTransitionEvent<>(System.currentTimeMillis(), prevState, newState));
  }

  @Override
  public boolean processMetricMessage(final String metricField, final byte[] metricValue) {
    // do nothing
    return false;
  }
}
