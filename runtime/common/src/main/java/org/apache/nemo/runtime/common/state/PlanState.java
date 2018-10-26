package org.apache.nemo.runtime.common.state;

import org.apache.nemo.common.StateMachine;

/**
 * Represents the states and their transitions of a physical plan.
 */
public final class PlanState {
  private final StateMachine stateMachine;

  public PlanState() {
    stateMachine = buildTaskStateMachine();
  }

  private StateMachine buildTaskStateMachine() {
    final StateMachine.Builder stateMachineBuilder = StateMachine.newBuilder();

    // Add states
    stateMachineBuilder.addState(State.READY, "The plan has been created and submitted to runtime.");
    stateMachineBuilder.addState(State.EXECUTING, "The plan is executing (with its stages executing).");
    stateMachineBuilder.addState(State.COMPLETE, "The plan is complete.");
    stateMachineBuilder.addState(State.FAILED, "Plan failed.");

    // Add transitions
    stateMachineBuilder.addTransition(State.READY, State.EXECUTING,
        "Begin executing!");
    stateMachineBuilder.addTransition(State.EXECUTING, State.COMPLETE,
        "All stages complete, plan complete");
    stateMachineBuilder.addTransition(State.EXECUTING, State.FAILED,
        "Unrecoverable failure in a stage");

    stateMachineBuilder.setInitialState(State.READY);

    return stateMachineBuilder.build();
  }

  public StateMachine getStateMachine() {
    return stateMachine;
  }

  /**
   * PlanState.
   */
  public enum State {
    READY,
    EXECUTING,
    COMPLETE,
    FAILED
  }

  @Override
  public String toString() {
    return stateMachine.getCurrentState().toString();
  }
}
