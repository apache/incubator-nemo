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

import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.executor.data.SerializerManager;
import edu.snu.nemo.runtime.executor.data.streamchainer.Serializer;

/**
 * This abstract class represents a default {@link BlockStore},
 * which contains other components used in each implementation of {@link BlockStore}.
 */
public abstract class AbstractBlockStore implements BlockStore {
  private final SerializerManager serializerManager;

  /**
   * Constructor.
   * @param serializerManager the coder manager.
   */
  protected AbstractBlockStore(final SerializerManager serializerManager) {
    this.serializerManager = serializerManager;
  }

  /**
   * Gets data coder for a block from the {@link SerializerManager}.
   *
   * @param blockId the ID of the block to get the coder.
   * @return the coder.
   */
  protected final Serializer getSerializerFromWorker(final String blockId) {
    final String runtimeEdgeId = RuntimeIdGenerator.getRuntimeEdgeIdFromBlockId(blockId);
    return serializerManager.getSerializer(runtimeEdgeId);
  }
}
