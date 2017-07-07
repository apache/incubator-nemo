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
    stateMachineBuilder.addState(State.MOVING, "The partition is moving.");
    stateMachineBuilder.addState(State.COMMITTED, "The partition has been committed.");
    stateMachineBuilder.addState(State.REMOVED, "The partition has been removed (e.g., GC-ed).");
    stateMachineBuilder.addState(State.LOST, "Partition lost.");

    // Add transitions
    stateMachineBuilder.addTransition(State.READY, State.COMMITTED, "Committed as soon as created");
    stateMachineBuilder.addTransition(State.READY, State.MOVING, "Partition moving");
    stateMachineBuilder.addTransition(State.MOVING, State.COMMITTED, "Successfully moved and committed");
    stateMachineBuilder.addTransition(State.MOVING, State.LOST, "Lost before committed");
    stateMachineBuilder.addTransition(State.COMMITTED, State.LOST, "Lost after committed");
    stateMachineBuilder.addTransition(State.COMMITTED, State.REMOVED, "Removed after committed");

    stateMachineBuilder.addTransition(State.COMMITTED, State.MOVING,
        "(WARNING) Possible race condition: receiver may have reached us before the sender, or there's sth wrong");

    stateMachineBuilder.addTransition(State.LOST, State.COMMITTED, "Recomputation successful and committed");

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
    MOVING,
    COMMITTED,
    /**
     * A data can be considered "lost" by PartitionManagerMaster for the following reasons:
     * 1) The executor that has the partition goes down
     * 2) Local data fetch failure (disk corruption, partition evicted due to memory pressure, etc)
     * 3) Remote partition fetch failure (network partitioning, network timeout, etc)
     *
     * Our current PartitionManager implementation does *not* properly handle the above cases.
     * They should be handled in the future with the issue, TODO #163: Handle Fault Tolerance
     *
     * Moreover, we assume that all lost partitions are recoverable,
     * meaning that we do not fail the job upon the event of a lost partition.
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
