package org.apache.nemo.compiler.frontend.beam.transform;

import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.TimerInternals;

/**
 * State and timer internal.
 */
public final class StateAndTimerForKey {
  public StateInternals stateInternals;
  private TimerInternals timerInternals;

  public StateAndTimerForKey(final StateInternals stateInternals,
                             final TimerInternals timerInternals) {
    this.stateInternals = stateInternals;
    this.timerInternals = timerInternals;
  }
}
