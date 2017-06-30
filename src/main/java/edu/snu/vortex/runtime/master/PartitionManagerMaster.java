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
package edu.snu.vortex.runtime.master;

import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.state.PartitionState;
import edu.snu.vortex.common.StateMachine;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Master-side partition manager.
 * For now, all its operations are synchronized to guarantee thread safety.
 */
@ThreadSafe
public final class PartitionManagerMaster {
  private static final Logger LOG = Logger.getLogger(PartitionManagerMaster.class.getName());
  private final Map<String, PartitionState> partitionIdToState;
  private final Map<String, String> committedPartitionIdToWorkerId;

  @Inject
  public PartitionManagerMaster() {
    this.partitionIdToState = new HashMap<>();
    this.committedPartitionIdToWorkerId = new HashMap<>();
  }

  public synchronized void initializeState(final String edgeId, final int srcTaskIndex) {
    final String partitionId = RuntimeIdGenerator.generatePartitionId(edgeId, srcTaskIndex);
    partitionIdToState.put(partitionId, new PartitionState());
  }

  public synchronized void initializeState(final String edgeId, final int srcTaskIndex, final int partitionIndex) {
    final String partitionId = RuntimeIdGenerator.generatePartitionId(edgeId, srcTaskIndex, partitionIndex);
    partitionIdToState.put(partitionId, new PartitionState());
  }

  public synchronized void removeWorker(final String executorId) {
    // Set partition states to lost
    committedPartitionIdToWorkerId.entrySet().stream()
        .filter(e -> e.getValue().equals(executorId))
        .map(Map.Entry::getKey)
        .forEach(partitionId -> onPartitionStateChanged(executorId, partitionId, PartitionState.State.LOST));

    // Update worker-related global variables
    committedPartitionIdToWorkerId.entrySet().removeIf(e -> e.getValue().equals(executorId));
  }

  public synchronized Optional<String> getPartitionLocation(final String partitionId) {
    final String executorId = committedPartitionIdToWorkerId.get(partitionId);
    return Optional.ofNullable(executorId);
  }

  public synchronized void onPartitionStateChanged(final String executorId,
                                                   final String partitionId,
                                                   final PartitionState.State newState) {
    final StateMachine sm = partitionIdToState.get(partitionId).getStateMachine();
    final Enum oldState = sm.getCurrentState();
    LOG.log(Level.FINE, "Partition State Transition: id {0} from {1} to {2}",
        new Object[]{partitionId, oldState, newState});

    sm.setState(newState);

    switch (newState) {
      case MOVING:
        if (oldState == PartitionState.State.COMMITTED) {
          LOG.log(Level.WARNING, "Transition from committed to moving: "
              + "reset to commited since receiver probably reached us before the sender");
          sm.setState(PartitionState.State.COMMITTED);
        }
        break;
      case COMMITTED:
        committedPartitionIdToWorkerId.put(partitionId, executorId);
        break;
      case REMOVED:
        committedPartitionIdToWorkerId.remove(partitionId);
        break;
      case LOST:
        throw new UnsupportedOperationException(newState.toString());
      default:
        throw new UnsupportedOperationException(newState.toString());
    }
  }
}
