/*
 * Copyright (C) 2018 Seoul National University
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

import edu.snu.nemo.common.exception.BlockFetchException;
import edu.snu.nemo.common.exception.BlockWriteException;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.runtime.executor.data.*;
import edu.snu.nemo.runtime.executor.data.block.Block;
import edu.snu.nemo.runtime.executor.data.streamchainer.Serializer;
import edu.snu.nemo.runtime.executor.data.metadata.LocalFileMetadata;
import edu.snu.nemo.runtime.executor.data.block.FileBlock;
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
