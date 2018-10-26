package org.apache.nemo.runtime.common.metric;

import java.io.Serializable;

/**
 * Event of state transition. It contains timestamp and the state transition.
 * @param <T> class of state for the metric.
 */
public final class StateTransitionEvent<T extends Serializable> extends Event {
  private T prevState;
  private T newState;

  public StateTransitionEvent(final long timestamp, final T prevState, final T newState) {
    super(timestamp);
    this.prevState = prevState;
    this.newState = newState;
  }

  /**
   * Get previous state.
   * @return previous state.
   */
  public T getPrevState() {
    return prevState;
  }

  /**
   * Get new state.
   * @return new state.
   */
  public T getNewState() {
    return newState;
  }
}
