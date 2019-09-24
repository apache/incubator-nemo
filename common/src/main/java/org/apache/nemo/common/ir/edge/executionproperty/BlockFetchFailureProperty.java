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
package org.apache.nemo.common.ir.edge.executionproperty;

import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;

/**
 * Decides how to react to a data block fetch failure.
 */
public final class BlockFetchFailureProperty extends EdgeExecutionProperty<BlockFetchFailureProperty.Value> {
  /**
   * Constructor.
   *
   * @param value value of the execution property.
   */
  private BlockFetchFailureProperty(final Value value) {
    super(value);
  }

  /**
   * Static method exposing the constructor.
   *
   * @param value value of the new execution property.
   * @return the newly created execution property.
   */
  public static BlockFetchFailureProperty of(final Value value) {
    return new BlockFetchFailureProperty(value);
  }

  /**
   * Possible values of DataFlowModel ExecutionProperty.
   */
  public enum Value {
    /**
     * (DEFAULT BEHAVIOR)
     * The task will be cancelled and retried by the scheduler.
     */
    CANCEL_TASK,

    /**
     * Do not cancel the running task.
     * Instead, retry fetching the data block two seconds after the fetch failure.
     *
     * We wait two seconds in the hope that the parent task will be re-scheduled by the master,
     * and make the block "available" again.
     * Upon the failure of the retry, we retry again. (i.e., we retry forever)
     */
    RETRY_AFTER_TWO_SECONDS_FOREVER,
  }
}
