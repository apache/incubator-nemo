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

import edu.snu.nemo.common.exception.BlockFetchException;
import edu.snu.nemo.conf.JobConf;
import edu.snu.nemo.common.exception.BlockWriteException;
import edu.snu.nemo.runtime.executor.data.*;
import edu.snu.nemo.runtime.executor.data.block.Block;
import edu.snu.nemo.runtime.executor.data.streamchainer.Serializer;
import edu.snu.nemo.runtime.executor.data.metadata.RemoteFileMetadata;
import edu.snu.nemo.runtime.executor.data.block.FileBlock;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

/**
 * Stores blocks in a mounted GlusterFS volume.
 * Because the data is stored in remote files and globally accessed by multiple nodes,
 * each read, or deletion for a file needs one instance of {@link FileBlock}.
 * When a remote file block is created, it's metadata is maintained in memory until the block is committed.
 * After the block is committed, the metadata is store in and read from a file.
 */
@ThreadSafe
public final class GlusterFileStore extends AbstractBlockStore implements RemoteFileStore {
  private final String fileDirectory;

  /**
   * Constructor.
   *
   * @param volumeDirectory   the remote volume directory which will contain the files.
   * @param jobId             the job id.
   * @param serializerManager the serializer manager.
   */
  @Inject
  private GlusterFileStore(@Parameter(JobConf.GlusterVolumeDirectory.class) final String volumeDirectory,
                           @Parameter(JobConf.JobId.class) final String jobId,
                           final SerializerManager serializerManager) {
    super(serializerManager);
    this.fileDirectory = volumeDirectory + "/" + jobId;
    new File(fileDirectory).mkdirs();
  }

  /**
   * @see BlockStore#createBlock(String).
   */
  @Override
  public Block createBlock(final String blockId) {
    deleteBlock(blockId);
    final Serializer serializer = getSerializerFromWorker(blockId);
    final String filePath = DataUtil.blockIdToFilePath(blockId, fileDirectory);
    final RemoteFileMetadata metadata =
        RemoteFileMetadata.create(DataUtil.blockIdToMetaFilePath(blockId, fileDirectory));
    return new FileBlock<>(blockId, serializer, filePath, metadata);
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
    // Do nothing. The block have to be written in the remote file during commit.
  }

  /**
   * Reads a committed block from this store.
   *
   * @param blockId of the target partition.
   * @return the target block (if it exists).
   * @throws BlockFetchException for any error occurred while trying to fetch a block.
   */
  @Override
  public Optional<Block> readBlock(final String blockId) throws BlockFetchException {
    final String filePath = DataUtil.blockIdToFilePath(blockId, fileDirectory);
    if (!new File(filePath).isFile()) {
      return Optional.empty();
    } else {
      try {
        final FileBlock block = getBlockFromFile(blockId);
        return Optional.of(block);
      } catch (final IOException e) {
        throw new BlockFetchException(e);
      }
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
      if (new File(filePath).isFile()) {
        final FileBlock block = getBlockFromFile(blockId);
        block.deleteFile();
        return true;
      } else {
        return false;
      }
    } catch (final IOException e) {
      throw new BlockFetchException(e);
    }
  }

  /**
   * Gets a {@link FileBlock} from the block and it's metadata file.
   * Because the data is stored in remote files and globally accessed by multiple nodes,
   * each read, or deletion for a file needs one instance of {@link FileBlock},
   * and the temporary block will not be maintained by this executor.
   *
   * @param blockId the ID of the block to get.
   * @param <K>     the type of the key of the block.
   * @return the {@link FileBlock} gotten.
   * @throws IOException if fail to get.
   */
  private <K extends Serializable> FileBlock<K> getBlockFromFile(final String blockId) throws IOException {
    final Serializer serializer = getSerializerFromWorker(blockId);
    final String filePath = DataUtil.blockIdToFilePath(blockId, fileDirectory);
    final RemoteFileMetadata<K> metadata =
        RemoteFileMetadata.open(DataUtil.blockIdToMetaFilePath(blockId, fileDirectory));
    return new FileBlock<>(blockId, serializer, filePath, metadata);
  }
}
