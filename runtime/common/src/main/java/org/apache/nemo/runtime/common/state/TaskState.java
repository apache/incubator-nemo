/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.common.state;

import org.apache.nemo.common.StateMachine;

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
    stateMachineBuilder.addState(State.READY, "The task is ready to be executed.");
    stateMachineBuilder.addState(State.EXECUTING, "The task is executing.");
    stateMachineBuilder.addState(State.ON_HOLD, "The task is paused (e.g., for dynamic optimization).");
    stateMachineBuilder.addState(State.COMPLETE, "The task has completed.");
    stateMachineBuilder.addState(State.SHOULD_RETRY, "The task should be retried.");
    stateMachineBuilder.addState(State.FAILED, "Task failed, and is unrecoverable. The job will fail.");

    // From READY
    stateMachineBuilder.addTransition(State.READY, State.EXECUTING, "Scheduling to executor");

    // From EXECUTING
    stateMachineBuilder.addTransition(State.EXECUTING, State.COMPLETE, "Task completed normally");
    stateMachineBuilder.addTransition(State.EXECUTING, State.ON_HOLD, "Task paused for dynamic optimization");
    stateMachineBuilder.addTransition(State.EXECUTING, State.SHOULD_RETRY, "Did not complete, should be retried");
    stateMachineBuilder.addTransition(State.EXECUTING, State.FAILED, "Unrecoverable failure");

    // From ON HOLD
    stateMachineBuilder.addTransition(State.ON_HOLD, State.COMPLETE, "Task completed after being on hold");
    stateMachineBuilder.addTransition(State.ON_HOLD, State.SHOULD_RETRY, "Did not complete, should be retried");
    stateMachineBuilder.addTransition(State.ON_HOLD, State.FAILED, "Unrecoverable failure");

    // From COMPLETE
    stateMachineBuilder.addTransition(State.COMPLETE, State.SHOULD_RETRY, "Completed before, but should be retried");

    // From SHOULD_RETRY
    stateMachineBuilder.addTransition(State.SHOULD_RETRY, State.SHOULD_RETRY,
        "SHOULD_RETRY can be caused by multiple reasons");

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
    ON_HOLD, // for dynamic optimization
    COMPLETE,
    SHOULD_RETRY,
    FAILED,
  }

  /**
   * Causes of a recoverable failure.
   */
  public enum RecoverableTaskFailureCause {
    INPUT_READ_FAILURE, // Occurs when a task is unable to read its input block
    OUTPUT_WRITE_FAILURE, // Occurs when a task successfully generates its output, but is unable to write it
  }

  @Override
  public String toString() {
    return stateMachine.getCurrentState().toString();
  }
}
