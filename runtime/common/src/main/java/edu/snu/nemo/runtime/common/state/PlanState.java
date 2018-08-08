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
package edu.snu.nemo.runtime.common.state;

import edu.snu.nemo.common.StateMachine;

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
    final StringBuffer sb = new StringBuffer();
    sb.append(stateMachine.getCurrentState());
    return sb.toString();
  }
}
