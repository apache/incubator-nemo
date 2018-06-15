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
 */
public final class StageState {
  private final StateMachine stateMachine;

  public StageState() {
    stateMachine = buildTaskStateMachine();
  }

  private StateMachine buildTaskStateMachine() {
    final StateMachine.Builder stateMachineBuilder = StateMachine.newBuilder();

    // Add states
    stateMachineBuilder.addState(State.READY, "The stage has been created.");
    stateMachineBuilder.addState(State.EXECUTING, "The stage is executing.");
    stateMachineBuilder.addState(State.COMPLETE, "All of this stage's tasks have completed.");
    stateMachineBuilder.addState(State.FAILED_RECOVERABLE, "Stage failed, but is recoverable.");
    stateMachineBuilder.addState(State.FAILED_UNRECOVERABLE, "Stage failed, and is unrecoverable. The job will fail.");

    // Add transitions
    stateMachineBuilder.addTransition(State.READY, State.EXECUTING,
        "The stage can now schedule its tasks");
    stateMachineBuilder.addTransition(State.READY, State.FAILED_UNRECOVERABLE,
        "Job Failure");

    stateMachineBuilder.addTransition(State.EXECUTING, State.COMPLETE,
        "All tasks complete");
    stateMachineBuilder.addTransition(State.EXECUTING, State.FAILED_UNRECOVERABLE,
        "Unrecoverable failure in a task");
    stateMachineBuilder.addTransition(State.EXECUTING, State.FAILED_RECOVERABLE,
        "Recoverable failure in a task");

    stateMachineBuilder.addTransition(State.COMPLETE, State.FAILED_RECOVERABLE,
        "Container on which the stage's output is stored failed");

    stateMachineBuilder.addTransition(State.FAILED_RECOVERABLE, State.READY,
        "Recoverable stage failure");
    stateMachineBuilder.addTransition(State.FAILED_RECOVERABLE, State.FAILED_UNRECOVERABLE,
        "");

    stateMachineBuilder.setInitialState(State.READY);

    return stateMachineBuilder.build();
  }

  public StateMachine getStateMachine() {
    return stateMachine;
  }

  /**
   * StageState.
   */
  public enum State {
    READY,
    EXECUTING,
    COMPLETE,
    FAILED_RECOVERABLE,
    FAILED_UNRECOVERABLE
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer();
    sb.append(stateMachine.getCurrentState());
    return sb.toString();
  }
}
