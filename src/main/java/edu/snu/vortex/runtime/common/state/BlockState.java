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
 * Represents the states of a whole block(a task output).
 */
public final class BlockState {
  private final StateMachine stateMachine;

  public BlockState() {
    stateMachine = buildBlockStateMachine();
  }

  private StateMachine buildBlockStateMachine() {
    final StateMachine.Builder stateMachineBuilder = StateMachine.newBuilder();

    // Add states
    stateMachineBuilder.addState(State.READY, "The block is ready to be created.");
    stateMachineBuilder.addState(State.MOVING, "The block is moving.");
    stateMachineBuilder.addState(State.COMMITTED, "The block has been committed.");
    stateMachineBuilder.addState(State.REMOVED, "The block has been removed (e.g., GC-ed).");
    stateMachineBuilder.addState(State.LOST, "Block lost.");

    // Add transitions
    stateMachineBuilder.addTransition(State.READY, State.COMMITTED, "Committed as soon as created");
    stateMachineBuilder.addTransition(State.READY, State.MOVING, "Block moving");
    stateMachineBuilder.addTransition(State.MOVING, State.COMMITTED, "Successfully moved and committed");
    stateMachineBuilder.addTransition(State.MOVING, State.LOST, "Lost before committed");
    stateMachineBuilder.addTransition(State.COMMITTED, State.LOST, "Lost after committed");
    stateMachineBuilder.addTransition(State.COMMITTED, State.REMOVED, "Removed after committed");

    stateMachineBuilder.addTransition(State.COMMITTED, State.MOVING,
        "(WARNING) Possible race condition: receiver may have reached us before the sender, or there's sth wrong");

    stateMachineBuilder.setInitialState(State.READY);

    return stateMachineBuilder.build();
  }

  public StateMachine getStateMachine() {
    return stateMachine;
  }

  /**
   * BlockState.
   */
  public enum State {
    READY,
    MOVING,
    COMMITTED,
    /**
     * A block can be considered "lost" by BlockManagerMaster for the following reasons:
     * 1) The executor that has the block goes down
     * 2) Local block fetch failure (disk corruption, block evicted due to memory pressure, etc)
     * 3) Remote block fetch failure (network partitioning, network timeout, etc)
     *
     * Our current BlockManager implementation does *not* properly handle the above cases.
     * They should be handled in the future with the issue, TODO #163: Handle Fault Tolerance
     *
     * Moreover, we assume that all lost blocks are recoverable,
     * meaning that we do not fail the job upon the event of a lost block.
     * Thus, we only have a single state(LOST) that represents failure.
     */
    LOST,
    REMOVED
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer();
    sb.append(stateMachine.getCurrentState());
    return sb.toString();
  }
}
