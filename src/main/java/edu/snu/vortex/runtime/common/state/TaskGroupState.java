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
 * Represents the states and their transitions of a {@link edu.snu.vortex.runtime.common.plan.physical.TaskGroup}.
 */
public final class TaskGroupState {
  private final StateMachine stateMachine;

  public TaskGroupState() {
    stateMachine = buildTaskGroupStateMachine();
  }

  private StateMachine buildTaskGroupStateMachine() {
    final StateMachine.Builder stateMachineBuilder = StateMachine.newBuilder();

    // Add states
    stateMachineBuilder.addState(State.READY, "The task group has been created.");
    stateMachineBuilder.addState(State.SCHEDULED_TO_EXECUTOR,
        "The task group has been scheduled to executor from master.");
    stateMachineBuilder.addState(State.EXECUTING, "The task group is executing (with one of its tasks).");
    stateMachineBuilder.addState(State.COMPLETE, "All of this task group's tasks have completed.");
    stateMachineBuilder.addState(State.FAILED, "Task Group failed.");

    // Add transitions
    stateMachineBuilder.addTransition(State.READY, State.SCHEDULED_TO_EXECUTOR,
        "Scheduling to executor");
    stateMachineBuilder.addTransition(State.SCHEDULED_TO_EXECUTOR, State.EXECUTING,
        "Begin executing!");
    stateMachineBuilder.addTransition(State.EXECUTING, State.COMPLETE,
        "All tasks complete");

    stateMachineBuilder.addTransition(State.READY, State.FAILED,
        "Master failure");
    stateMachineBuilder.addTransition(State.SCHEDULED_TO_EXECUTOR, State.FAILED,
        "Executor failure");
    stateMachineBuilder.addTransition(State.EXECUTING, State.FAILED,
        "Executor failure");

    stateMachineBuilder.setInitialState(State.READY);

    return stateMachineBuilder.build();
  }

  /**
   * Task Group states.
   */
  public enum State {
    READY,
    SCHEDULED_TO_EXECUTOR,
    EXECUTING,
    COMPLETE,
    FAILED
  }
}
