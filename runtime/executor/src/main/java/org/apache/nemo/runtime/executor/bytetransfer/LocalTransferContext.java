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
package org.apache.nemo.runtime.executor.bytetransfer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class LocalTransferContext {
  private static final Logger LOG = LoggerFactory.getLogger(ByteTransferContext.class);
  private final String executorId;
  private final String edgeId;
  private final int srcTaskIndex;
  private final int dstTaskIndex;
  private final boolean isLocal = true;

  LocalTransferContext(final String executorId,
                       final String edgeId,
                       final int srcTaskIndex,
                       final int dstTaskIndex) {
    this.executorId = executorId;
    this.edgeId = edgeId;
    this.srcTaskIndex = srcTaskIndex;
    this.dstTaskIndex = dstTaskIndex;
  }

  public abstract void close();

  public boolean isLocal() {
    return isLocal;
  }

  public String getExecutorId() {
    return executorId;
  }

  public String getEdgeId() {
    return edgeId;
  }

  public int getSrcTaskIndex() {
    return srcTaskIndex;
  }

  public int getDstTaskIndex() {
    return dstTaskIndex;
  }

}
