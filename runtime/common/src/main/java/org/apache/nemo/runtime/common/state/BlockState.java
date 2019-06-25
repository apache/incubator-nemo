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
    stateMachineBuilder.addState(State.IN_PROGRESS, "The block is in the progress of being created.");
    stateMachineBuilder.addState(State.AVAILABLE, "The block is available.");
    stateMachineBuilder.addState(State.NOT_AVAILABLE, "The block is not available.");

    // From IN_PROGRESS
    stateMachineBuilder.addTransition(State.IN_PROGRESS, State.AVAILABLE, "The block is successfully created");
    stateMachineBuilder.addTransition(State.IN_PROGRESS, State.NOT_AVAILABLE,
      "The block is lost before being created");

    // From AVAILABLE
    stateMachineBuilder.addTransition(State.AVAILABLE, State.NOT_AVAILABLE, "The block is not available");

    stateMachineBuilder.setInitialState(State.IN_PROGRESS);

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
    return stateMachine.getCurrentState().toString();
  }
}
