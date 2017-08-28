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
 * Represents the states of a whole data(a task output).
 */
public final class PartitionState {
  private final StateMachine stateMachine;

  public PartitionState() {
    stateMachine = buildPartitionStateMachine();
  }

  private StateMachine buildPartitionStateMachine() {
    final StateMachine.Builder stateMachineBuilder = StateMachine.newBuilder();

    // Add states
    stateMachineBuilder.addState(State.READY, "The partition is ready to be created.");
    stateMachineBuilder.addState(State.SCHEDULED, "The partition is scheduled for creation.");
    stateMachineBuilder.addState(State.COMMITTED, "The partition has been committed.");
    // TODO #444: Introduce BlockState -> remove PARTIAL_COMMITTED and manage the block state in PartitionStores.
    stateMachineBuilder.addState(State.PARTIAL_COMMITTED, "The partition has been partially committed.");
    stateMachineBuilder.addState(State.LOST_BEFORE_COMMIT, "The task group that produces the partition is scheduled, "
        + "but failed before committing");
    stateMachineBuilder.addState(State.REMOVED, "The partition has been removed (e.g., GC-ed).");
    stateMachineBuilder.addState(State.LOST, "Partition lost.");

    // Add transitions
    stateMachineBuilder.addTransition(State.READY, State.SCHEDULED,
        "The task group that produces the partition is scheduled.");
    stateMachineBuilder.addTransition(State.SCHEDULED, State.COMMITTED, "Successfully moved and committed");
    stateMachineBuilder.addTransition(State.SCHEDULED, State.PARTIAL_COMMITTED,
        "Some part of this partition is successfully moved and committed");
    stateMachineBuilder.addTransition(State.SCHEDULED, State.LOST_BEFORE_COMMIT, "The partition is lost before commit");
    stateMachineBuilder.addTransition(State.PARTIAL_COMMITTED, State.LOST_BEFORE_COMMIT,
        "The partition is lost before commit");
    stateMachineBuilder.addTransition(State.PARTIAL_COMMITTED, State.COMMITTED,
        "Whole partition is successfully moved and committed");
    stateMachineBuilder.addTransition(State.PARTIAL_COMMITTED, State.PARTIAL_COMMITTED,
        "Another part of this partition is successfully moved and committed");
    stateMachineBuilder.addTransition(State.COMMITTED, State.LOST, "Lost after committed");
    stateMachineBuilder.addTransition(State.COMMITTED, State.REMOVED, "Removed after committed");
    stateMachineBuilder.addTransition(State.REMOVED, State.SCHEDULED,
        "Re-scheduled after removal due to fault tolerance");

    stateMachineBuilder.addTransition(State.LOST, State.SCHEDULED, "The producer of the lost partition is rescheduled");
    stateMachineBuilder.addTransition(State.LOST_BEFORE_COMMIT, State.SCHEDULED,
        "The producer of the lost partition is rescheduled");

    stateMachineBuilder.setInitialState(State.READY);

    return stateMachineBuilder.build();
  }

  public StateMachine getStateMachine() {
    return stateMachine;
  }

  /**
   * PartitionState.
   */
  public enum State {
    READY,
    SCHEDULED,
    COMMITTED,
    // TODO #444: Introduce BlockState -> remove PARTIAL_COMMITTED and manage the block state in PartitionStores.
    PARTIAL_COMMITTED,
    LOST_BEFORE_COMMIT,
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
