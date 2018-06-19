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
 * Represents the states of a whole data(a task output).
 */
public final class BlockState {
  private final StateMachine stateMachine;

  public BlockState() {
    stateMachine = buildBlockStateMachine();
  }

  private StateMachine buildBlockStateMachine() {
    final StateMachine.Builder stateMachineBuilder = StateMachine.newBuilder();

    // Add states
    stateMachineBuilder.addState(State.NOT_AVAILABLE, "The block is not available.");
    stateMachineBuilder.addState(State.IN_PROGRESS, "The block is in the progress of being created.");
    stateMachineBuilder.addState(State.AVAILABLE, "The block is available.");

    // Add transitions
    stateMachineBuilder.addTransition(State.NOT_AVAILABLE, State.IN_PROGRESS,
        "The task that produces the block is scheduled.");
    stateMachineBuilder.addTransition(State.IN_PROGRESS, State.AVAILABLE, "The block is successfully created");

    stateMachineBuilder.addTransition(State.IN_PROGRESS, State.NOT_AVAILABLE,
        "The block is lost before being created");
    stateMachineBuilder.addTransition(State.AVAILABLE, State.NOT_AVAILABLE, "The block is lost");
    stateMachineBuilder.addTransition(State.NOT_AVAILABLE, State.NOT_AVAILABLE,
        "A block can be reported lost from multiple sources");

    stateMachineBuilder.setInitialState(State.NOT_AVAILABLE);

    return stateMachineBuilder.build();
  }

  public StateMachine getStateMachine() {
    return stateMachine;
  }

  /**
   * BlockState.
   */
  public enum State {
    NOT_AVAILABLE,
    IN_PROGRESS,
    AVAILABLE,
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer();
    sb.append(stateMachine.getCurrentState());
    return sb.toString();
  }
}
