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
package edu.snu.nemo.runtime.common.exception;

import edu.snu.nemo.runtime.common.state.BlockState;

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
