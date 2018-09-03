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
 * Represents the states and their transitions of a stage.
 *
 * Maintained as simple two (INCOMPLETE, COMPLETE) states to avoid ambiguity when the tasks are in different states.
 * For example it is not clear whether a stage should be EXECUTING or SHOULD_RESTART, if one of the tasks in the stage
 * is EXECUTING, and another is SHOULD_RESTART.
 */
public final class StageState {
  private final StateMachine stateMachine;

  public StageState() {
    stateMachine = buildTaskStateMachine();
  }

  private StateMachine buildTaskStateMachine() {
    final StateMachine.Builder stateMachineBuilder = StateMachine.newBuilder();

    // Add states
    stateMachineBuilder.addState(State.INCOMPLETE, "Some tasks in this stage are not complete.");
    stateMachineBuilder.addState(State.COMPLETE, "All of this stage's tasks have completed.");

    // Add transitions
    stateMachineBuilder.addTransition(
        State.INCOMPLETE, State.INCOMPLETE, "A task in the stage needs to be retried");
    stateMachineBuilder.addTransition(State.INCOMPLETE, State.COMPLETE, "All tasks complete");
    stateMachineBuilder.addTransition(State.COMPLETE, State.INCOMPLETE,
        "Completed before, but a task in this stage should be retried");
    stateMachineBuilder.addTransition(State.COMPLETE, State.COMPLETE,
      "Completed before, but probably a cloned task has completed again");

    stateMachineBuilder.setInitialState(State.INCOMPLETE);

    return stateMachineBuilder.build();
  }

  public StateMachine getStateMachine() {
    return stateMachine;
  }

  /**
   * StageState.
   */
  public enum State {
    INCOMPLETE,
    COMPLETE
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer();
    sb.append(stateMachine.getCurrentState());
    return sb.toString();
  }
}
