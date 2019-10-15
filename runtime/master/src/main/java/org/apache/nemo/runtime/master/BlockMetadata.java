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
package org.apache.nemo.runtime.master;

import org.apache.nemo.common.StateMachine;
import org.apache.nemo.common.exception.IllegalStateTransitionException;
import org.apache.nemo.runtime.common.exception.AbsentBlockException;
import org.apache.nemo.runtime.common.state.BlockState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents a block metadata stored in the metadata server.
 */
@ThreadSafe
final class BlockMetadata {
  private static final Logger LOG = LoggerFactory.getLogger(BlockMetadata.class.getName());
  private final String blockId;
  private final BlockState blockState;
  private final BlockManagerMaster.BlockRequestHandler locationHandler;

  /**
   * Constructs the metadata for a block.
   *
   * @param blockId the id of the block.
   */
  BlockMetadata(final String blockId) {
    // Initialize block level metadata.
    this.blockId = blockId;
    this.blockState = new BlockState();
    this.locationHandler = new BlockManagerMaster.BlockRequestHandler(blockId);
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
      case IN_PROGRESS:
        break;
      case NOT_AVAILABLE:
        locationHandler.completeExceptionally(new AbsentBlockException(blockId, newState));
        break;
      case AVAILABLE:
        if (location == null) {
          throw new RuntimeException("Null location");
        }
        locationHandler.complete(location);
        break;
      default:
        throw new UnsupportedOperationException(newState.toString());
    }

    try {
      stateMachine.setState(newState);
    } catch (IllegalStateTransitionException e) {
      throw new RuntimeException(blockId + " - Illegal block state transition ", e);
    }
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
  BlockState.State getBlockState() {
    return (BlockState.State) blockState.getStateMachine().getCurrentState();
  }

  /**
   * @return the handler of block location requests.
   */
  synchronized BlockManagerMaster.BlockRequestHandler getLocationHandler() {
    return locationHandler;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(blockId);
    sb.append("(");
    sb.append(blockState);
    sb.append(")");
    return sb.toString();
  }

  @Override
  public boolean equals(final Object that) {
    if (!(that instanceof BlockMetadata)) {
      return false;
    }
    return this.blockId.equals(((BlockMetadata) that).getBlockId());
  }

  @Override
  public int hashCode() {
    return blockId.hashCode();
  }
}
