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
package org.apache.nemo.runtime.executor.data.stores;

//import org.apache.nemo.runtime.executor.data.BlockManagerWorker;
import org.apache.nemo.runtime.executor.data.MemoryPoolAssigner;
import org.apache.nemo.common.exception.BlockWriteException;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.data.block.Block;
import org.apache.nemo.runtime.executor.data.block.NonSerializedMemoryBlock;
import org.apache.nemo.runtime.executor.data.partition.NonSerializedPartition;
import org.apache.nemo.runtime.executor.data.streamchainer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

/**
 * Store data in local memory.
 */
@ThreadSafe
public final class MemoryStore extends LocalBlockStore {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryStore.class.getName());


  /**
   * Constructor.
   *
   * @param serializerManager the serializer manager.
   * @param memoryPoolAssigner the memory pool assigner.
   */
  @Inject
  private MemoryStore(final SerializerManager serializerManager,
                      final MemoryPoolAssigner memoryPoolAssigner) {
    super(serializerManager, memoryPoolAssigner);
  }

  /**
   * @see BlockStore#createBlock(String)
   */
  @Override
  public NonSerializedMemoryBlock createBlock(final String blockId) {
    final Serializer serializer = getSerializerFromWorker(blockId);
    LOG.info("dongjoo MemoryStore, createBlock serializer {}, blockId {}", serializer, blockId);
    return new NonSerializedMemoryBlock(blockId, serializer, getMemoryPoolAssigner());
  }

  /**
   * Writes a committed block to this store.
   *
   * @param block the block to write.
   * @throws BlockWriteException if fail to write.
   */
  @Override
  public void writeBlock(final Block block) {
    LOG.info("MemoryStore, writeBlock block {}", block);
    if (!(block instanceof NonSerializedMemoryBlock)) {
      throw new BlockWriteException(new Throwable(
        this.toString() + "only accept " + NonSerializedPartition.class.getName()));
    } else if (!block.isCommitted()) {
      throw new BlockWriteException(new Throwable("The block " + block.getId() + "is not committed yet."));
    } else {
      getBlockMap().put(block.getId(), block);
    }
  }

  /**
   * @see BlockStore#deleteBlock(String)
   */
  @Override
  public boolean deleteBlock(final String blockId) {
    return getBlockMap().remove(blockId) != null;
  }
}
