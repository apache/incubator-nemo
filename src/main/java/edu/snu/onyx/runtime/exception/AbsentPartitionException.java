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
package edu.snu.onyx.runtime.exception;

import edu.snu.onyx.runtime.common.state.PartitionState;

/**
 * An exception which represents the requested partition is neither COMMITTED nor SCHEDULED.
 */
public final class AbsentPartitionException extends Exception {
  private final String partitionId;
  private final PartitionState.State state;

  /**
   * @param partitionId id of the partition
   * @param state state of the partition
   */
  public AbsentPartitionException(final String partitionId, final PartitionState.State state) {
    this.partitionId = partitionId;
    this.state = state;
  }

  /**
   * @return id of the partition
   */
  public String getPartitionId() {
    return partitionId;
  }

  /**
   * @return state of the partition
   */
  public PartitionState.State getState() {
    return state;
  }
}
