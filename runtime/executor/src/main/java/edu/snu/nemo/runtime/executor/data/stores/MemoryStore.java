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

import edu.snu.nemo.runtime.executor.data.SerializerManager;
import edu.snu.nemo.runtime.executor.data.block.NonSerializedMemoryBlock;
import edu.snu.nemo.runtime.executor.data.streamchainer.Serializer;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

/**
 * Store data in local memory.
 */
@ThreadSafe
public final class MemoryStore extends LocalBlockStore {

  /**
   * Constructor.
   *
   * @param serializerManager the serializer manager.
   */
  @Inject
  private MemoryStore(final SerializerManager serializerManager) {
    super(serializerManager);
  }

  /**
   * @see BlockStore#createBlock(String)
   */
  @Override
  public void createBlock(final String blockId) {
    final Serializer serializer = getSerializerFromWorker(blockId);
    getBlockMap().put(blockId, new NonSerializedMemoryBlock(serializer));
  }

  /**
   * @see BlockStore#removeBlock(String)
   */
  @Override
  public boolean removeBlock(final String blockId) {
    return getBlockMap().remove(blockId) != null;
  }
}
