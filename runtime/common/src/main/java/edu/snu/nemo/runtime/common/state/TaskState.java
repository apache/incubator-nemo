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
 * Represents the states and their transitions of a task.
 */
public final class TaskState {
  private final StateMachine stateMachine;

  public TaskState() {
    stateMachine = buildTaskStateMachine();
  }

  private StateMachine buildTaskStateMachine() {
    final StateMachine.Builder stateMachineBuilder = StateMachine.newBuilder();

    // Add states
    stateMachineBuilder.addState(State.READY, "The task has been created.");
    stateMachineBuilder.addState(State.EXECUTING, "The task is executing.");
    stateMachineBuilder.addState(State.COMPLETE, "The task has completed.");
    stateMachineBuilder.addState(State.FAILED_RECOVERABLE, "Task failed, but is recoverable.");
    stateMachineBuilder.addState(State.FAILED_UNRECOVERABLE,
        "Task failed, and is unrecoverable. The job will fail.");
    stateMachineBuilder.addState(State.ON_HOLD, "The task is paused for dynamic optimization.");

    // Add transitions
    stateMachineBuilder.addTransition(State.READY, State.EXECUTING,
        "Scheduling to executor");
    stateMachineBuilder.addTransition(State.READY, State.FAILED_RECOVERABLE,
        "Stage Failure by a recoverable failure in another task");
    stateMachineBuilder.addTransition(State.READY, State.FAILED_UNRECOVERABLE,
        "Stage Failure");

    stateMachineBuilder.addTransition(State.EXECUTING, State.COMPLETE,
        "All tasks complete");
    stateMachineBuilder.addTransition(State.EXECUTING, State.FAILED_UNRECOVERABLE,
        "Unrecoverable failure in a task/Executor failure");
    stateMachineBuilder.addTransition(State.EXECUTING, State.FAILED_RECOVERABLE,
        "Recoverable failure in a task/Container failure");
    stateMachineBuilder.addTransition(State.EXECUTING, State.ON_HOLD, "Task paused for dynamic optimization");
    stateMachineBuilder.addTransition(State.ON_HOLD, State.COMPLETE, "Task completed after dynamic optimization");

    stateMachineBuilder.addTransition(State.COMPLETE, State.FAILED_RECOVERABLE,
        "Recoverable failure in a task/Container failure");

    stateMachineBuilder.addTransition(State.FAILED_RECOVERABLE, State.READY,
        "Recovered from failure and is ready");
    stateMachineBuilder.addTransition(State.FAILED_RECOVERABLE, State.FAILED_UNRECOVERABLE,
        "");

    stateMachineBuilder.setInitialState(State.READY);

    return stateMachineBuilder.build();
  }

  public StateMachine getStateMachine() {
    return stateMachine;
  }

  /**
   * TaskState.
   */
  public enum State {
    READY,
    EXECUTING,
    COMPLETE,
    FAILED_RECOVERABLE,
    FAILED_UNRECOVERABLE,
    ON_HOLD, // for dynamic optimization
  }

  /**
   * Causes of a recoverable failure.
   */
  public enum RecoverableFailureCause {
    INPUT_READ_FAILURE, // Occurs when a task is unable to read its input block
    OUTPUT_WRITE_FAILURE, // Occurs when a task successfully generates its output, but is unable to write it
    CONTAINER_FAILURE // When a REEF evaluator fails
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer();
    sb.append(stateMachine.getCurrentState());
    return sb.toString();
  }
}
