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
package edu.snu.coral.runtime.executor.data.stores;

import edu.snu.coral.common.exception.BlockFetchException;
import edu.snu.coral.common.exception.BlockWriteException;
import edu.snu.coral.conf.JobConf;
import edu.snu.coral.runtime.common.data.KeyRange;
import edu.snu.coral.runtime.executor.data.*;
import edu.snu.coral.runtime.executor.data.block.Block;
import edu.snu.coral.runtime.executor.data.block.FileBlock;
import edu.snu.coral.runtime.executor.data.metadata.LocalFileMetadata;
import edu.snu.coral.runtime.executor.data.streamchainer.Serializer;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stores blocks in local files, using zram as temporary directory.
 */
public final class LocalFileStoreWithZram extends AbstractBlockStore implements FileStore {

  // A map between block id and data blocks.
  private final Map<String, Block> blockMap;
  private final String fileDirectory;
  private final String zramDirectory;

  /**
   * Constructor.
   *
   * @param fileDirectory the directory which will contain the files.
   * @param zramDirectory the directory which will contain temporary files.
   * @param serializerManager  the serializer manager.
   */
  @Inject
  private LocalFileStoreWithZram(@Parameter(JobConf.FileDirectory.class) final String fileDirectory,
                                 @Parameter(JobConf.ZramDirectory.class) final String zramDirectory,
                                 final SerializerManager serializerManager) {
    super(serializerManager);
    this.blockMap = new ConcurrentHashMap<>();
    this.fileDirectory = fileDirectory;
    this.zramDirectory = zramDirectory;
    new File(fileDirectory).mkdirs();
    new File(zramDirectory).mkdirs();
  }

  /**
   * Creates a new block.
   *
   * @param blockId the ID of the block to create.
   * @see BlockStore#createBlock(String)
   */
  @Override
  public void createBlock(final String blockId) {
    removeBlock(blockId);

    final Serializer serializer = getSerializerFromWorker(blockId);
    final LocalFileMetadata metadata = new LocalFileMetadata();

    final FileBlock block =
        new FileBlock(serializer, DataUtil.blockIdToFilePath(blockId, zramDirectory), metadata);
    getBlockMap().put(blockId, block);
  }

  /**
   * Removes the file that the target block is stored.
   *
   * @param blockId of the block.
   * @return whether the block exists or not.
   */
  @Override
  public Boolean removeBlock(final String blockId) throws BlockFetchException {
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

  /**
   * @see FileStore#getFileAreas(String, KeyRange)
   */
  @Override
  public List<FileArea> getFileAreas(final String blockId,
                                     final KeyRange keyRange) {
    try {
      final FileBlock block = (FileBlock) getBlockMap().get(blockId);
      if (block == null) {
        throw new IOException(String.format("%s does not exists", blockId));
      }
      return block.asFileAreas(keyRange);
    } catch (final IOException retrievalException) {
      throw new BlockFetchException(retrievalException);
    }
  }

  /**
   * @see BlockStore#commitBlock(String)
   */
  @Override
  public void commitBlock(final String blockId) {
    final Serializer serializer = getSerializerFromWorker(blockId);
    final FileBlock oldBlock = (FileBlock) getBlockMap().get(blockId);
    if (oldBlock == null) {
      throw new BlockWriteException(new Throwable("There isn't any block with id " + blockId));
    }
    final String oldPath = DataUtil.blockIdToFilePath(blockId, zramDirectory);
    final String newPath = DataUtil.blockIdToFilePath(blockId, fileDirectory);
    final FileBlock newBlock = new FileBlock(serializer, newPath, oldBlock.getMetadata());
    try {
      Files.move(new File(oldPath).toPath(), new File(newPath).toPath());
      newBlock.commit();
    } catch (final IOException e) {
      throw new BlockWriteException(e);
    }
    getBlockMap().put(blockId, newBlock);
  }

  /**
   * @see BlockStore#putPartitions(String, Iterable)
   */
  @Override
  public <K extends Serializable>
  Optional<List<Long>> putPartitions(final String blockId,
                                     final Iterable<NonSerializedPartition<K>> partitions)
      throws BlockWriteException {
    try {
      final Block<K> block = blockMap.get(blockId);
      if (block == null) {
        throw new BlockWriteException(new Throwable("The block " + blockId + "is not created yet."));
      }
      return block.putPartitions(partitions);
    } catch (final IOException e) {
      throw new BlockWriteException(new Throwable("Failed to store partitions to this block."));
    }
  }

  /**
   * @see BlockStore#putSerializedPartitions(String, Iterable)
   */
  @Override
  public <K extends Serializable>
  List<Long> putSerializedPartitions(final String blockId,
                                     final Iterable<SerializedPartition<K>> partitions) {
    try {
      final Block<K> block = blockMap.get(blockId);
      if (block == null) {
        throw new BlockWriteException(new Throwable("The block " + blockId + "is not created yet."));
      }
      return block.putSerializedPartitions(partitions);
    } catch (final IOException e) {
      throw new BlockWriteException(new Throwable("Failed to store partitions to this block."));
    }
  }

  /**
   * @see BlockStore#getPartitions(String, KeyRange)
   */
  @Override
  public <K extends Serializable>
  Optional<Iterable<NonSerializedPartition<K>>> getPartitions(final String blockId, final KeyRange<K> keyRange) {
    final Block<K> block = blockMap.get(blockId);

    if (block != null) {
      try {
        final Iterable<NonSerializedPartition<K>> partitionsInRange = block.getPartitions(keyRange);
        return Optional.of(partitionsInRange);
      } catch (final IOException e) {
        throw new BlockFetchException(e);
      }
    } else {
      return Optional.empty();
    }
  }

  /**
   * @see BlockStore#getSerializedPartitions(String, edu.snu.coral.runtime.common.data.KeyRange)
   */
  @Override
  public <K extends Serializable>
  Optional<Iterable<SerializedPartition<K>>> getSerializedPartitions(final String blockId, final KeyRange<K> keyRange) {
    final Block<K> block = blockMap.get(blockId);

    if (block != null) {
      try {
        final Iterable<SerializedPartition<K>> partitionsInRange = block.getSerializedPartitions(keyRange);
        return Optional.of(partitionsInRange);
      } catch (final IOException e) {
        throw new BlockFetchException(e);
      }
    } else {
      return Optional.empty();
    }
  }

  /**
   * @return the map between the IDs and {@link Block}.
   */
  public Map<String, Block> getBlockMap() {
    return blockMap;
  }
}
