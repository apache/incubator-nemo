package org.apache.nemo.client;

import org.apache.nemo.runtime.common.state.PlanState;
import org.apache.nemo.runtime.master.PlanStateManager;

import java.util.concurrent.TimeUnit;

/**
 * A request endpoint in driver side of a plan.
 */
public final class DriverEndpoint {

  /**
   * The {@link PlanStateManager} of the running plan.
   */
  private final PlanStateManager planStateManager;

  /**
   * The {@link ClientEndpoint} of the plan.
   */
  private final ClientEndpoint clientEndpoint;

  /**
   * Construct an endpoint in driver side.
   * This method will be called by {@link ClientEndpoint}.
   * @param planStateManager of running plan.
   * @param clientEndpoint of running plan.
   */
  public DriverEndpoint(final PlanStateManager planStateManager,
                        final ClientEndpoint clientEndpoint) {
    this.planStateManager = planStateManager;
    this.clientEndpoint = clientEndpoint;
    clientEndpoint.connectDriver(this);
  }

  /**
   * Get the current state of the running plan.
   * This method will be called by {@link ClientEndpoint}.
   * @return the current state of the running plan.
   */
  PlanState.State getState() {
    return planStateManager.getPlanState();
  }

  /**
   * Wait for this plan to be finished and return the final state.
   * It wait for at most the given time.
   * This method will be called by {@link ClientEndpoint}.
   * @param timeout of waiting.
   * @param unit of the timeout.
   * @return the final state of this plan.
   */
  PlanState.State waitUntilFinish(final long timeout,
                                  final TimeUnit unit) {
    return planStateManager.waitUntilFinish(timeout, unit);
  }

  /**
   * Wait for this plan to be finished and return the final state.
   * This method will be called by {@link ClientEndpoint}.
   * @return the final state of this plan.
   */
  PlanState.State waitUntilFinish() {
    return planStateManager.waitUntilFinish();
  }
}
