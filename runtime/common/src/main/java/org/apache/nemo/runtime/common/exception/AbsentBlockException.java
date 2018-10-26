package org.apache.nemo.runtime.common.exception;

import org.apache.nemo.runtime.common.state.BlockState;

/**
 * An exception which represents the requested block is neither AVAILABLE nor IN_PROGRESS.
 */
public final class AbsentBlockException extends Exception {
  private final String blockId;
  private final BlockState.State state;

  /**
   * @param blockId id of the block
   * @param state  state of the block
   */
  public AbsentBlockException(final String blockId, final BlockState.State state) {
    this.blockId = blockId;
    this.state = state;
  }

  /**
   * @return id of the block
   */
  public String getBlockId() {
    return blockId;
  }

  /**
   * @return state of the block
   */
  public BlockState.State getState() {
    return state;
  }
}
