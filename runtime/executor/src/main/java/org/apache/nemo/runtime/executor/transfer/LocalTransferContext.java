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
package org.apache.nemo.runtime.executor.transfer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents the data transfer context when both the sender and the receiver are in the same executor.
 * The data transfer via local transfer contexts doesn't involve data serialization and deserialization.
 */
public abstract class LocalTransferContext {
  private static final Logger LOG = LoggerFactory.getLogger(LocalTransferContext.class);
  private final String executorId;
  private final String edgeId;
  private final int srcTaskIndex;
  private final int dstTaskIndex;

  /**
   * Creates a new local transfer context.
   * @param executorId id of the executor to which this context belongs
   * @param edgeId id of the DAG edge
   * @param srcTaskIndex source task index
   * @param dstTaskIndex destination task index
   */
  LocalTransferContext(final String executorId,
                       final String edgeId,
                       final int srcTaskIndex,
                       final int dstTaskIndex) {
    this.executorId = executorId;
    this.edgeId = edgeId;
    this.srcTaskIndex = srcTaskIndex;
    this.dstTaskIndex = dstTaskIndex;
  }

  /**
   * Accessor method for the executor id.
   * @return executor id
   */
  public final String getExecutorId() {
    return executorId;
  }

  /**
   * Accessor method for the edge id.
   * @return edge id
   */
  public final String getEdgeId() {
    return edgeId;
  }

  /**
   * Accessor method for the source task index.
   * @return source task index
   */
  public final int getSrcTaskIndex() {
    return srcTaskIndex;
  }

  /**
   * Accessor method for the destination task index.
   * @return destination task index
   */
  public final int getDstTaskIndex() {
    return dstTaskIndex;
  }
}
