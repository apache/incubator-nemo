package org.apache.nemo.runtime.common.metric;

import java.io.Serializable;
import java.util.List;

/**
 * Interface for metric which contians its state.
 * @param <T> class of state of the metric.
 */
public interface StateMetric<T extends Serializable> extends Metric {
  /**
   * Get its list of {@link StateTransitionEvent}.
   * @return list of events.
   */
  List<StateTransitionEvent<T>> getStateTransitionEvents();

  /**
   * Add a {@link StateTransitionEvent} to the metric.
   * @param prevState previous state.
   * @param newState new state.
   */
  void addEvent(final T prevState, final T newState);
}
