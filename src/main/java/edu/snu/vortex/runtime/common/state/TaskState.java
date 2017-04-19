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

import edu.snu.vortex.utils.StateMachine;

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
    stateMachineBuilder.addState(State.SCHEDULED_TO_EXECUTOR, "The task has been scheduled to executor from master.");
    stateMachineBuilder.addState(State.SCHEDULED_IN_EXECUTOR, "The task has been scheduled in its executor.");
    stateMachineBuilder.addState(State.EXECUTING, "The task is executing.");
    stateMachineBuilder.addState(State.COMPUTED, "The task's operation has been executed/computed.");
    stateMachineBuilder.addState(State.OUTPUT_COMMITTED, "The task's output has been committed.");
    stateMachineBuilder.addState(State.COMPLETE, "The task's execution is complete with its output committed.");
    stateMachineBuilder.addState(State.FAILED, "Task failed.");

    // Add transitions
    stateMachineBuilder.addTransition(State.READY, State.SCHEDULED_TO_EXECUTOR,
        "Scheduling to executor");
    stateMachineBuilder.addTransition(State.SCHEDULED_TO_EXECUTOR, State.SCHEDULED_IN_EXECUTOR,
        "Scheduling for execution");
    stateMachineBuilder.addTransition(State.SCHEDULED_IN_EXECUTOR, State.EXECUTING,
        "Begin executing!");
    stateMachineBuilder.addTransition(State.EXECUTING, State.COMPUTED,
        "Task computed");
    stateMachineBuilder.addTransition(State.COMPUTED, State.OUTPUT_COMMITTED,
        "Output written out");
    stateMachineBuilder.addTransition(State.OUTPUT_COMMITTED, State.COMPLETE,
        "Task is complete");

    stateMachineBuilder.addTransition(State.READY, State.FAILED,
        "Master failure");
    stateMachineBuilder.addTransition(State.SCHEDULED_TO_EXECUTOR, State.FAILED,
        "Executor failure");
    stateMachineBuilder.addTransition(State.SCHEDULED_IN_EXECUTOR, State.FAILED,
        "Executor failure");
    stateMachineBuilder.addTransition(State.EXECUTING, State.FAILED,
        "Executor failure");
    stateMachineBuilder.addTransition(State.COMPUTED, State.FAILED,
        "Executor failure");
    stateMachineBuilder.addTransition(State.OUTPUT_COMMITTED, State.FAILED,
        "Executor failure");

    stateMachineBuilder.setInitialState(State.READY);

    return stateMachineBuilder.build();
  }

  /**
   * Task states.
   */
  public enum State {
    READY,
    SCHEDULED_TO_EXECUTOR,
    SCHEDULED_IN_EXECUTOR,
    EXECUTING,
    COMPUTED,
    OUTPUT_COMMITTED,
    COMPLETE,
    FAILED
  }
}
