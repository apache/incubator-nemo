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
import edu.snu.vortex.runtime.common.state.BlockState;
import edu.snu.vortex.utils.StateMachine;

import javax.annotation.concurrent.ThreadSafe;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Matser-side block manager.
 * For now, all its operations are synchronized to guarantee thread safety.
 */
@ThreadSafe
public final class BlockManagerMaster {
  private static final Logger LOG = Logger.getLogger(BlockManagerMaster.class.getName());
  private final Map<String, BlockState> blockIdToState;
  private final Map<String, String> committedBlockIdToWorkerId;

  public BlockManagerMaster() {
    this.blockIdToState = new HashMap<>();
    this.committedBlockIdToWorkerId = new HashMap<>();
  }

  public synchronized void initializeState(final String edgeId, final int srcTaskIndex) {
    final String blockId = RuntimeIdGenerator.generateBlockId(edgeId, srcTaskIndex);
    blockIdToState.put(blockId, new BlockState());
  }

  public synchronized void initializeState(final String edgeId, final int srcTaskIndex, final int partitionIndex) {
    final String blockId = RuntimeIdGenerator.generateBlockId(edgeId, srcTaskIndex, partitionIndex);
    blockIdToState.put(blockId, new BlockState());
  }

  public synchronized void removeWorker(final String executorId) {
    // Set block states to lost
    committedBlockIdToWorkerId.entrySet().stream()
        .filter(e -> e.getValue().equals(executorId))
        .map(Map.Entry::getKey)
        .forEach(blockId -> onBlockStateChanged(executorId, blockId, BlockState.State.LOST));

    // Update worker-related global variables
    committedBlockIdToWorkerId.entrySet().removeIf(e -> e.getValue().equals(executorId));
  }

  public synchronized Optional<String> getBlockLocation(final String blockId) {
    final String executorId = committedBlockIdToWorkerId.get(blockId);
    if (executorId == null) {
      return Optional.empty();
    } else {
      return Optional.ofNullable(executorId);
    }
  }

  public synchronized void onBlockStateChanged(final String executorId,
                                               final String blockId,
                                               final BlockState.State newState) {
    final StateMachine sm = blockIdToState.get(blockId).getStateMachine();
    final Enum oldState = sm.getCurrentState();
    LOG.log(Level.FINE, "Block State Transition: id {0} from {1} to {2}", new Object[]{blockId, oldState, newState});

    sm.setState(newState);

    switch (newState) {
      case MOVING:
        if (oldState == BlockState.State.COMMITTED) {
          LOG.log(Level.WARNING, "Transition from committed to moving: "
              + "reset to commited since receiver probably reached us before the sender");
          sm.setState(BlockState.State.COMMITTED);
        }
        break;
      case COMMITTED:
        committedBlockIdToWorkerId.put(blockId, executorId);
        break;
      case LOST:
        throw new UnsupportedOperationException(newState.toString());
      default:
        throw new UnsupportedOperationException(newState.toString());
    }
  }
}
