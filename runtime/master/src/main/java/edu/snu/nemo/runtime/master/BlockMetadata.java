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
package edu.snu.nemo.runtime.master;

import edu.snu.nemo.common.StateMachine;
import edu.snu.nemo.runtime.common.state.BlockState;
import edu.snu.nemo.runtime.common.exception.AbsentBlockException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.CompletableFuture;

/**
 * This class represents a block metadata stored in the metadata server.
 */
@ThreadSafe
final class BlockMetadata {
  // Partition level metadata.
  private static final Logger LOG = LoggerFactory.getLogger(BlockManagerMaster.class.getName());
  private final String blockId;
  private final BlockState blockState;
  private volatile CompletableFuture<String> locationFuture; // the future of the location of this block.

  /**
   * Constructs the metadata for a block.
   *
   * @param blockId the id of the block.
   */
  BlockMetadata(final String blockId) {
    // Initialize block level metadata.
    this.blockId = blockId;
    this.blockState = new BlockState();
    this.locationFuture = new CompletableFuture<>();
  }

  /**
   * Deals with state change of the corresponding block.
   *
   * @param newState the new state of the block.
   * @param location the location of the block (e.g., worker id, remote store).
   *                 {@code null} if not committed or lost.
   */
  synchronized void onStateChanged(final BlockState.State newState,
                                   @Nullable final String location) {
    final StateMachine stateMachine = blockState.getStateMachine();
    final Enum oldState = stateMachine.getCurrentState();
    LOG.debug("Block State Transition: id {} from {} to {}", new Object[]{blockId, oldState, newState});

    switch (newState) {
      case SCHEDULED:
        stateMachine.setState(newState);
        break;
      case LOST:
        LOG.info("Block {} lost in {}", new Object[]{blockId, location});
      case LOST_BEFORE_COMMIT:
      case REMOVED:
        // Reset the block location and committer information.
        locationFuture.completeExceptionally(new AbsentBlockException(blockId, newState));
        locationFuture = new CompletableFuture<>();
        stateMachine.setState(newState);
        break;
      case COMMITTED:
        assert (location != null);
        completeLocationFuture(location);
        stateMachine.setState(newState);
        break;
      default:
        throw new UnsupportedOperationException(newState.toString());
    }
  }

  /**
   * Completes the location future of this block.
   *
   * @param location the location of the block.
   */
  private synchronized void completeLocationFuture(final String location) {
    locationFuture.complete(location);
  }

  /**
   * @return the block id.
   */
  String getBlockId() {
    return blockId;
  }

  /**
   * @return the state of this block.
   */
  BlockState getBlockState() {
    return blockState;
  }

  /**
   * @return the future of the location of this Block.
   */
  synchronized CompletableFuture<String> getLocationFuture() {
    return locationFuture;
  }
}
