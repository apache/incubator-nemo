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

import org.apache.nemo.runtime.executor.data.MemoryPoolAssigner;
import org.apache.nemo.common.exception.BlockFetchException;
import org.apache.nemo.common.exception.BlockWriteException;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.apache.nemo.runtime.executor.data.block.Block;
import org.apache.nemo.runtime.executor.data.block.FileBlock;
import org.apache.nemo.runtime.executor.data.metadata.LocalFileMetadata;
import org.apache.nemo.runtime.executor.data.streamchainer.Serializer;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.File;
import java.io.IOException;

/**
 * Stores blocks in local files.
 */
@ThreadSafe
public final class LocalFileStore extends LocalBlockStore {
  private final String fileDirectory;

  /**
   * Constructor.
   *
   * @param fileDirectory     the directory which will contain the files.
   * @param serializerManager the serializer manager.
   * @param memoryPoolAssigner the memory pool assigner.
   */
  @Inject
  private LocalFileStore(@Parameter(JobConf.FileDirectory.class) final String fileDirectory,
                         final SerializerManager serializerManager,
                         final MemoryPoolAssigner memoryPoolAssigner) {
    super(serializerManager, memoryPoolAssigner);
    this.fileDirectory = fileDirectory;
    new File(fileDirectory).mkdirs();
  }

  @Override
  public Block createBlock(final String blockId) {
    deleteBlock(blockId);

    final Serializer serializer = getSerializerFromWorker(blockId);
    final LocalFileMetadata metadata = new LocalFileMetadata();

    return new FileBlock(blockId, serializer, DataUtil.blockIdToFilePath(blockId, fileDirectory),
                                                                  metadata, getMemoryPoolAssigner());
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
