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

package org.apache.nemo.runtime.executor.data;
/**
 * Pool to keep track of Storage Memory.
 */
public class StorageMemoryPool {
  private long poolSize;
  private long memoryUsed;

  /**
   * release memory back into the storage pool.
   * @param memory the amount of memory to release.
   */
  public void releaseMemory(final long memory) {
    this.memoryUsed -= memory;
  }

  /**
   * Acquire memory from pool for caching.
   * @param blockId The id of the block to be cached
   * @param numBytestoAcquire size of block
   * @return successfully acquired or not
   */
  public boolean acquireMemory(final String blockId, final long numBytestoAcquire) {
    if (this.poolSize - this.memoryUsed > 0) {
      this.memoryUsed += numBytestoAcquire;
      return true;
    }
    return false;
  }

  /**
   * Checkout how much memory is remaining.
   * @return the remaining amount of memory
   */
  public long getRemainingMemory() {
    return this.poolSize - this.memoryUsed;
  }

  /**
   * Get the total pool size.
   * @return the pool size
   */
  public long getPoolSize() {
    return this.poolSize;
  }

  StorageMemoryPool(final long storageMemoryLimit) {
    this.poolSize = storageMemoryLimit;
  }

}
