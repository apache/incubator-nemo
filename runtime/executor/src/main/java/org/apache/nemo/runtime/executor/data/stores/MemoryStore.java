package org.apache.nemo.runtime.executor.data.stores;

import org.apache.nemo.common.exception.BlockWriteException;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.data.block.Block;
import org.apache.nemo.runtime.executor.data.block.NonSerializedMemoryBlock;
import org.apache.nemo.runtime.executor.data.partition.NonSerializedPartition;
import org.apache.nemo.runtime.executor.data.streamchainer.Serializer;

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
  public NonSerializedMemoryBlock createBlock(final String blockId) {
    final Serializer serializer = getSerializerFromWorker(blockId);
    return new NonSerializedMemoryBlock(blockId, serializer);
  }

  /**
   * Writes a committed block to this store.
   *
   * @param block the block to write.
   * @throws BlockWriteException if fail to write.
   */
  @Override
  public void writeBlock(final Block block) throws BlockWriteException {
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
