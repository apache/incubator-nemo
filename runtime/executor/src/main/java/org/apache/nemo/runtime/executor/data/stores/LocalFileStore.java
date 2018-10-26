package org.apache.nemo.runtime.executor.data.stores;

import org.apache.nemo.common.exception.BlockFetchException;
import org.apache.nemo.common.exception.BlockWriteException;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.executor.data.*;
import org.apache.nemo.runtime.executor.data.block.Block;
import org.apache.nemo.runtime.executor.data.streamchainer.Serializer;
import org.apache.nemo.runtime.executor.data.metadata.LocalFileMetadata;
import org.apache.nemo.runtime.executor.data.block.FileBlock;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.*;

/**
 * Stores blocks in local files.
 */
@ThreadSafe
public final class LocalFileStore extends LocalBlockStore {
  private final String fileDirectory;

  /**
   * Constructor.
   *
   * @param fileDirectory the directory which will contain the files.
   * @param serializerManager  the serializer manager.
   */
  @Inject
  private LocalFileStore(@Parameter(JobConf.FileDirectory.class) final String fileDirectory,
                         final SerializerManager serializerManager) {
    super(serializerManager);
    this.fileDirectory = fileDirectory;
    new File(fileDirectory).mkdirs();
  }

  /**
   * @see BlockStore#createBlock(String).
   */
  @Override
  public Block createBlock(final String blockId) {
    deleteBlock(blockId);

    final Serializer serializer = getSerializerFromWorker(blockId);
    final LocalFileMetadata metadata = new LocalFileMetadata();

    return new FileBlock(blockId, serializer, DataUtil.blockIdToFilePath(blockId, fileDirectory), metadata);
  }

  /**
   * Writes a committed block to this store.
   *
   * @param block the block to write.
   * @throws BlockWriteException if fail to write.
   */
  @Override
  public void writeBlock(final Block block) throws BlockWriteException {
    if (!(block instanceof FileBlock)) {
      throw new BlockWriteException(new Throwable(
          this.toString() + "only accept " + FileBlock.class.getName()));
    } else if (!block.isCommitted()) {
      throw new BlockWriteException(new Throwable("The block " + block.getId() + "is not committed yet."));
    } else {
      getBlockMap().put(block.getId(), block);
    }
  }

  /**
   * Removes the file that the target block is stored.
   *
   * @param blockId of the block.
   * @return whether the block exists or not.
   */
  @Override
  public boolean deleteBlock(final String blockId) throws BlockFetchException {
    final FileBlock fileBlock = (FileBlock) getBlockMap().remove(blockId);
    if (fileBlock == null) {
      return false;
    }
    try {
      fileBlock.deleteFile();
    } catch (final IOException e) {
      throw new BlockFetchException(e);
    }
    return true;
  }
}
