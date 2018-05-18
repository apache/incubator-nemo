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
package edu.snu.nemo.runtime.executor.data.stores;

import edu.snu.nemo.common.exception.BlockWriteException;
import edu.snu.nemo.runtime.executor.data.SerializerManager;
import edu.snu.nemo.runtime.executor.data.block.Block;
import edu.snu.nemo.runtime.executor.data.block.SerializedMemoryBlock;
import edu.snu.nemo.runtime.executor.data.streamchainer.Serializer;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

/**
 * Serialize and store data in local memory.
 */
@ThreadSafe
public final class SerializedMemoryStore extends LocalBlockStore {

  /**
   * Constructor.
   * @param serializerManager the serializer manager.
   */
  @Inject
  private SerializedMemoryStore(final SerializerManager serializerManager) {
    super(serializerManager);
  }

  /**
   * @see BlockStore#createBlock(String)
   */
  @Override
  public Block createBlock(final String blockId) {
    final Serializer serializer = getSerializerFromWorker(blockId);
    return new SerializedMemoryBlock(blockId, serializer);
  }

  /**
   * Writes a committed block to this store.
   *
   * @param block the block to write.
   * @throws BlockWriteException if fail to write.
   */
  @Override
  public void writeBlock(final Block block) throws BlockWriteException {
    if (!(block instanceof SerializedMemoryBlock)) {
      throw new BlockWriteException(new Throwable(
          this.toString() + "only accept " + SerializedMemoryBlock.class.getName()));
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
