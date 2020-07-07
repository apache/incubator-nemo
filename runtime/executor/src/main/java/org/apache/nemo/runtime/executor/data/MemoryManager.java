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

//import org.apache.nemo.common.exception.BlockWriteException;
//import org.apache.nemo.runtime.executor.data.block.Block;
//import org.apache.nemo.runtime.executor.data.block.NonSerializedMemoryBlock;
//import org.apache.nemo.runtime.executor.data.partition.NonSerializedPartition;
//import org.apache.nemo.runtime.executor.data.streamchainer.Serializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
//import java.util.concurrent.atomic.AtomicInteger;
//import javax.inject.Inject;

/**
 * MemoryManager for sharing the storage between Execution and Storage(caching).
 * writing to Partitions through MemoryStore must go through MemoryManager
 * to ensure that there is enough memory. If not, logic to handle spill to disk
 */
@ThreadSafe
public final class MemoryManager {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryManager.class.getName());
  // for testing purposes only
  private static int uniqueId = 42;

  private static long testStorageMemoryLimit;
  private StorageMemoryPool storageMemoryPool = new StorageMemoryPool();

  /**
   * Constructor.
   */
  public MemoryManager(final long testStorageMemoryLimit) {
    this.testStorageMemoryLimit = testStorageMemoryLimit;
  }

  @Inject
  public MemoryManager() {
    this.testStorageMemoryLimit = 10000;
  }

  public boolean acquireStorageMemory(final long mem) {
    LOG.info("MemoryManager, testStorageMemoryLimit used to be {}", this.testStorageMemoryLimit);
    this.testStorageMemoryLimit -= mem;
    LOG.info("MemoryManager, testStorageMemoryLimit is now {}", this.testStorageMemoryLimit);
    return this.testStorageMemoryLimit > 0;
  }

  public int getUniqueId() {
    return this.uniqueId;
  }
}
