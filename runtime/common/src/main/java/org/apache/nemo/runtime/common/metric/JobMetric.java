package org.apache.nemo.runtime.common.metric;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.runtime.common.plan.PhysicalPlan;
import org.apache.nemo.runtime.common.state.PlanState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Metric class for Job (or {@link PhysicalPlan}).
 */
public final class JobMetric implements StateMetric<PlanState.State> {
  private String id;
  private List<StateTransitionEvent<PlanState.State>> stateTransitionEvents = new ArrayList<>();
  private JsonNode stageDagJson;

  public JobMetric(final PhysicalPlan physicalPlan) {
    this.id = physicalPlan.getPlanId();
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
