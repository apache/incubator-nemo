/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.vortex.runtime.common.state;

import edu.snu.vortex.common.StateMachine;

/**
 * Represents the states and their transitions of a {@link edu.snu.vortex.runtime.common.plan.physical.Task}.
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
    stateMachineBuilder.addState(State.PENDING_IN_EXECUTOR, "The task is pending in its executor.");
    stateMachineBuilder.addState(State.EXECUTING, "The task is executing.");
    stateMachineBuilder.addState(State.COMPLETE, "The task's execution is complete with its output committed.");
    stateMachineBuilder.addState(State.FAILED_RECOVERABLE, "Task failed, but is recoverable.");
    stateMachineBuilder.addState(State.FAILED_UNRECOVERABLE, "Task failed, and is unrecoverable. The job will fail.");

    // Add transitions
    stateMachineBuilder.addTransition(State.READY, State.PENDING_IN_EXECUTOR,
        "Scheduling for execution");
    stateMachineBuilder.addTransition(State.READY, State.FAILED_UNRECOVERABLE,
        "Unrecoverable TaskGroup Failure");
    stateMachineBuilder.addTransition(State.READY, State.FAILED_RECOVERABLE,
        "Recoverable TaskGroup Failure");

    stateMachineBuilder.addTransition(State.PENDING_IN_EXECUTOR, State.EXECUTING,
        "Begin executing!");
    stateMachineBuilder.addTransition(State.PENDING_IN_EXECUTOR, State.FAILED_UNRECOVERABLE,
        "Unrecoverable TaskGroup Failure/Executor Failure");
    stateMachineBuilder.addTransition(State.PENDING_IN_EXECUTOR, State.FAILED_RECOVERABLE,
        "Recoverable TaskGroup Failure/Container Failure");

    stateMachineBuilder.addTransition(State.EXECUTING, State.COMPLETE,
        "Task computation done");
    stateMachineBuilder.addTransition(State.EXECUTING, State.FAILED_UNRECOVERABLE,
        "Unexpected failure/Executor Failure");
    stateMachineBuilder.addTransition(State.EXECUTING, State.FAILED_RECOVERABLE,
        "Container Failure");

    stateMachineBuilder.addTransition(State.COMPLETE, State.FAILED_UNRECOVERABLE,
        "Executor Failure");

    stateMachineBuilder.addTransition(State.COMPLETE, State.FAILED_RECOVERABLE,
        "Container Failure");

    stateMachineBuilder.addTransition(State.FAILED_RECOVERABLE, State.FAILED_UNRECOVERABLE,
        "");
    stateMachineBuilder.addTransition(State.FAILED_RECOVERABLE, State.READY,
        "Recoverable Task Failure");

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
    // PENDING_IN_EXECUTOR and EXECUTING states are only managed in executor.
    PENDING_IN_EXECUTOR,
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
