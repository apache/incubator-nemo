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

import org.apache.crail.*;
import org.apache.crail.conf.CrailConfiguration;
import org.apache.nemo.common.exception.BlockFetchException;
import org.apache.nemo.conf.JobConf;
import org.apache.nemo.common.exception.BlockWriteException;
import org.apache.nemo.runtime.executor.data.*;
import org.apache.nemo.runtime.executor.data.block.Block;
import org.apache.nemo.runtime.executor.data.block.FileBlock;
import org.apache.nemo.runtime.executor.data.metadata.CrailFileMetadata;
import org.apache.nemo.runtime.executor.data.streamchainer.Serializer;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

/**
 * Stores blocks in CrailStore.
 * Since the data is stored in CrailStore and globally accessed by multiple nodes,
 * each read, or deletion for a file needs one instance of {@link FileBlock}.
 * When FileBlock in Crail is created, it's metadata is maintained in memory until the block is committed.
 * After the block is committed, the metadata is stored in and read from a CrailStore.
 */
@ThreadSafe
public final class CrailFileStore extends AbstractBlockStore implements RemoteFileStore {
  private final String fileDirectory;
  private final CrailConfiguration conf;
  private final CrailStore fs;

  /**
   * Constructor.
   *
   * @param volumeDirectory   the CrailStore directory where we contain the files.
   * @param jobId             the job id.
   * @param serializerManager the serializer manager.
   * @throws Exception for any error occurred while trying to set Crail requirements.
   */
  @Inject
  private CrailFileStore(@Parameter(JobConf.CrailVolumeDirectory.class) final String volumeDirectory,
                         @Parameter(JobConf.JobId.class) final String jobId,
                         final SerializerManager serializerManager) throws Exception {
    super(serializerManager);
    this.conf = new CrailConfiguration();
    this.fs = CrailStore.newInstance(conf);
    this.fileDirectory = volumeDirectory;
  }

  @Override
  public Block createBlock(final String blockId) {
    deleteBlock(blockId);
    final Serializer serializer = getSerializerFromWorker(blockId);
    final String filePath = DataUtil.blockIdToFilePath(blockId, fileDirectory);
    final String metaPath = DataUtil.blockIdToMetaFilePath(blockId, fileDirectory);
    final CrailFileMetadata metadata = CrailFileMetadata.create(metaPath, fs);
    return new FileBlock<>(blockId, serializer, filePath, metadata, fs);
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
        this.toString() + " only accept " + FileBlock.class.getName()));
    } else if (!block.isCommitted()) {
      throw new BlockWriteException(new Throwable("The block " + block.getId() + "is not committed yet."));
    }
    // Do nothing. The block have to be written in CrailStore file during commit.
  }

  /**
   * Reads a committed block from this store.
   *
   * @param blockId of the target partition.
   * @return the target block (if it exists).
   * @throws BlockFetchException for any error occurred while trying to fetch a block.
   */

  public Optional<Block> readBlock(final String blockId) throws BlockFetchException {
    final String filePath = DataUtil.blockIdToFilePath(blockId, fileDirectory);
    try {
      if (fs.lookup(filePath).get() == null) {
        return Optional.empty();
      } else {
        try {
          final FileBlock block = getBlockFromFile(blockId);
          return Optional.of(block);
        } catch (final IOException e) {
          throw new BlockFetchException(e);
        } catch (Exception e) {
          throw new BlockFetchException(e);
        }
      }
    } catch (Exception e) {
      throw new BlockFetchException(e);
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
    final String filePath = DataUtil.blockIdToFilePath(blockId, fileDirectory);

    try {
      if (fs.lookup(filePath).get() != null) {
        final FileBlock block = getBlockFromFile(blockId);
        block.deleteFile();
        return true;
      } else {
        return false;
      }
    } catch (final IOException e) {
      throw new BlockFetchException(e);
    } catch (final Exception e) {
      throw new BlockFetchException(e);
    }
  }

  /**
   * Gets a {@link FileBlock} from the block and it's metadata file.
   * Because the data is stored in CrailStore and globally accessed by multiple nodes,
   * each read, or deletion for a file needs one instance of {@link FileBlock},
   * and the temporary block will not be maintained by this executor.
   *
   * @param blockId the ID of the block to get.
   * @param <K>     the type of the key of the block.
   * @return the {@link FileBlock} gotten.
   * @throws Exception if fail to get.
   */
  private <K extends Serializable> FileBlock<K> getBlockFromFile(final String blockId) throws Exception {
    final Serializer serializer = getSerializerFromWorker(blockId);
    final String filePath = DataUtil.blockIdToFilePath(blockId, fileDirectory);
    final CrailFileMetadata<K> metadata =
      CrailFileMetadata.open(DataUtil.blockIdToMetaFilePath(blockId, fileDirectory), fs);
    return new FileBlock<>(blockId, serializer, filePath, metadata, fs);
  }
}
