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

import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.executor.data.MemoryPoolAssigner;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.data.streamchainer.Serializer;

/**
 * This abstract class represents a default {@link BlockStore},
 * which contains other components used in each implementation of {@link BlockStore}.
 */
public abstract class AbstractBlockStore implements BlockStore {
  private final SerializerManager serializerManager;
  private final MemoryPoolAssigner memoryPoolAssigner;

  /**
   * Constructor.
   *
   * @param serializerManager the coder manager.
   * @param memoryPoolAssigner the memory pool assigner.
   */
  AbstractBlockStore(final SerializerManager serializerManager,
                               final MemoryPoolAssigner memoryPoolAssigner) {
    this.serializerManager = serializerManager;
    this.memoryPoolAssigner = memoryPoolAssigner;
  }

  /**
   * Gets data coder for a block from the {@link SerializerManager}.
   *
   * @param blockId the ID of the block to get the coder.
   * @return the coder.
   */
  final Serializer getSerializerFromWorker(final String blockId) {
    final String runtimeEdgeId = RuntimeIdManager.getRuntimeEdgeIdFromBlockId(blockId);
    return serializerManager.getSerializer(runtimeEdgeId);
  }

  /**
   * Gets the memory pool assigner for this executor.
   *
   * @return the memory pool assigner.
   */
  final MemoryPoolAssigner getMemoryPoolAssigner() {
    return memoryPoolAssigner;
  }
}
