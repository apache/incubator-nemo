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

import org.apache.nemo.conf.JobConf;
import org.apache.reef.tang.annotations.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.UUID;
//import java.util.concurrent.atomic.AtomicInteger;

/**
 * MemoryManager for sharing the storage between Execution and Storage(caching).
 * writing to Partitions through MemoryStore must go through MemoryManager
 * to ensure that there is enough memory, if not, BlockOutputWriter will spill the data to disk
 */
@ThreadSafe
public final class MemoryManager {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryManager.class.getName());
  // for testing purposes only
  private UUID uniqueId = UUID.randomUUID();
  private StorageMemoryPool storageMemoryPool;
  // maintains a hash map of blockIDs to size of the block,
  // used to release memory back when blocks are no longer needed
  private HashMap<String, Long> blockIdtoSize;

  public void releaseStorageMemory(final long memory) {
    this.storageMemoryPool.releaseMemory(memory);
  }

  @Inject
  public MemoryManager(@Parameter(JobConf.ExecutorMemoryMb.class) final int memory,
                       @Parameter(JobConf.StoragePoolRatio.class) final double storagePoolRatio) {
    long storageMemory = (long) (memory * storagePoolRatio) * 1024;
    this.storageMemoryPool = new StorageMemoryPool(storageMemory);
//    LOG.info("MemoryManage Executor memory mb {}", memory);
//    LOG.info("MemoryManager inject constructor called, storageMemoryLimit is {}",
//      this.storageMemoryPool.getPoolSize());
  }

  public long getRemainingStorageMemory() {
    return this.storageMemoryPool.getRemainingMemory();
  }

  public boolean acquireStorageMemory(final String blockId, final long mem) {
//    LOG.info("MemoryManager, testStorageMemoryLimit used to be {}", this.storageMemoryPool.getRemainingMemory());
    this.storageMemoryPool.acquireMemory(blockId, mem);
//    LOG.info("MemoryManager, testStorageMemoryLimit is now {}", this.storageMemoryPool.getRemainingMemory());
    return this.storageMemoryPool.getRemainingMemory() > 0;
  }

  public UUID getUniqueId() {
    return this.uniqueId;
  }
}

