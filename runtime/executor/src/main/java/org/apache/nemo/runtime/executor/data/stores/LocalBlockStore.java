package org.apache.nemo.runtime.executor.data.stores;

import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.data.block.Block;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This abstract class represents {@link BlockStore}
 * which contains the (meta)data of the {@link Block}s in local.
 * Because of this, store can maintain all blocks in a single map (mapped with their IDs).
 */
public abstract class LocalBlockStore extends AbstractBlockStore {
  // A map between block id and data blocks.
  private final Map<String, Block> blockMap;

  /**
   * Constructor.
   *
   * @param coderManager the coder manager.
   */
  protected LocalBlockStore(final SerializerManager coderManager) {
    super(coderManager);
    this.blockMap = new ConcurrentHashMap<>();
  }

  /**
   * Reads a committed block from this store.
   *
   * @param blockId of the target partition.
   * @return the target block (if it exists).
   */
  @Override
  public final Optional<Block> readBlock(final String blockId) {
    final Block block = blockMap.get(blockId);
    return block == null ? Optional.empty() : Optional.of(block);
  }

  /**
   * @return the map between the IDs and {@link Block}.
   */
  protected final Map<String, Block> getBlockMap() {
    return blockMap;
  }
}
