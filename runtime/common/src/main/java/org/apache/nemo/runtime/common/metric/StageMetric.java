package org.apache.nemo.runtime.common.metric;

import org.apache.nemo.runtime.common.plan.Stage;
import org.apache.nemo.runtime.common.state.StageState;

import java.util.ArrayList;
import java.util.List;

/**
 * Metric class for {@link Stage}.
 */
public class StageMetric implements StateMetric<StageState.State> {
  private String id;
  private List<StateTransitionEvent<StageState.State>> stateTransitionEvents = new ArrayList<>();

  public StageMetric(final Stage stage) {
    this.id = stage.getId();
  }

  public StageMetric(final String id) {
    this.id = id;
  }

  @Override
  public final String getId() {
    return id;
  }

  @Override
  public final List<StateTransitionEvent<StageState.State>> getStateTransitionEvents() {
    return stateTransitionEvents;
  }

  @Override
  public final void addEvent(final StageState.State prevState, final StageState.State newState) {
    stateTransitionEvents.add(new StateTransitionEvent<>(System.currentTimeMillis(), prevState, newState));
  }

  @Override
  public final boolean processMetricMessage(final String metricField, final byte[] metricValue) {
    // do nothing
    return false;
  }
}
