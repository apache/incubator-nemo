package org.apache.nemo.runtime.executor.data.stores;

import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.data.streamchainer.Serializer;

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
    final String runtimeEdgeId = RuntimeIdManager.getRuntimeEdgeIdFromBlockId(blockId);
    return serializerManager.getSerializer(runtimeEdgeId);
  }
}
