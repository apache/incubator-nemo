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
import edu.snu.nemo.runtime.common.data.KeyRange;
import edu.snu.nemo.runtime.executor.data.*;
import edu.snu.nemo.runtime.executor.data.block.Block;
import edu.snu.nemo.runtime.executor.data.partition.NonSerializedPartition;
import edu.snu.nemo.runtime.executor.data.partition.SerializedPartition;
import edu.snu.nemo.runtime.executor.data.streamchainer.Serializer;
import edu.snu.nemo.runtime.executor.data.metadata.RemoteFileMetadata;
import edu.snu.nemo.runtime.executor.data.block.FileBlock;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stores blocks in a mounted GlusterFS volume.
 * Because the data is stored in remote files and globally accessed by multiple nodes,
 * each read, or deletion for a file needs one instance of {@link FileBlock}.
 * When a remote file block is created, it's metadata is maintained in memory until the block is committed.
 * After the block is committed, the metadata is store in and read from a file.
 */
@ThreadSafe
public final class GlusterFileStore extends AbstractBlockStore implements RemoteFileStore {
  private final Map<String, FileBlock> blockMap;
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
    this.blockMap = new ConcurrentHashMap<>();
    this.fileDirectory = volumeDirectory + "/" + jobId;
    new File(fileDirectory).mkdirs();
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
    final String filePath = DataUtil.blockIdToFilePath(blockId, fileDirectory);
    final RemoteFileMetadata metadata =
        RemoteFileMetadata.create(DataUtil.blockIdToMetaFilePath(blockId, fileDirectory));
    final FileBlock block = new FileBlock<>(serializer, filePath, metadata);
    blockMap.put(blockId, block);
  }

  /**
   * @see BlockStore#write(String, Serializable, Object).
   */
  @Override
  public <K extends Serializable> void write(final String blockId,
                                             final K key,
                                             final Object element) throws BlockWriteException {
    try {
      final Block<K> block = blockMap.get(blockId);
      if (block == null) {
        throw new BlockWriteException(new Throwable("The block " + blockId + "is not created yet."));
      }
      block.write(key, element);
    } catch (final IOException e) {
      throw new BlockWriteException(new Throwable("Failed to store partitions to this block."));
    }
  }

  /**
   * @see BlockStore#writePartitions(String, Iterable)
   */
  @Override
  public <K extends Serializable> void writePartitions(final String blockId,
                                                       final Iterable<NonSerializedPartition<K>> partitions)
      throws BlockWriteException {
    try {
      final Block<K> block = blockMap.get(blockId);
      if (block == null) {
        throw new BlockWriteException(new Throwable("The block " + blockId + "is not created yet."));
      }
      block.writePartitions(partitions);
    } catch (final IOException e) {
      throw new BlockWriteException(new Throwable("Failed to store partitions to this block."));
    }
  }

  /**
   * @see BlockStore#writeSerializedPartitions(String, Iterable)
   */
  @Override
  public <K extends Serializable> void writeSerializedPartitions(final String blockId,
                                                                 final Iterable<SerializedPartition<K>> partitions) {
    try {
      final Block<K> block = blockMap.get(blockId);
      if (block == null) {
        throw new BlockWriteException(new Throwable("The block " + blockId + "is not created yet."));
      }
      block.writeSerializedPartitions(partitions);
    } catch (final IOException e) {
      throw new BlockWriteException(new Throwable("Failed to store partitions to this block."));
    }
  }

  /**
   * Retrieves {@link NonSerializedPartition}s in a specific {@link KeyRange} from a block.
   *
   * @see BlockStore#readPartitions(String, KeyRange)
   */
  @Override
  public <K extends Serializable> Optional<Iterable<NonSerializedPartition<K>>> readPartitions(
      final String blockId,
      final KeyRange<K> keyRange) throws BlockFetchException {
    final String filePath = DataUtil.blockIdToFilePath(blockId, fileDirectory);
    if (!new File(filePath).isFile()) {
      return Optional.empty();
    } else {
      // Deserialize the target data in the corresponding file.
      try {
        final FileBlock<K> block = getBlockFromFile(blockId);
        final Iterable<NonSerializedPartition<K>> partitionsInRange = block.readPartitions(keyRange);
        return Optional.of(partitionsInRange);
      } catch (final IOException e) {
        throw new BlockFetchException(e);
      }
    }
  }

  /**
   * @see BlockStore#readSerializedPartitions(String, KeyRange)
   */
  @Override
  public <K extends Serializable>
  Optional<Iterable<SerializedPartition<K>>> readSerializedPartitions(final String blockId,
                                                                      final KeyRange<K> keyRange) {
    final String filePath = DataUtil.blockIdToFilePath(blockId, fileDirectory);
    if (!new File(filePath).isFile()) {
      return Optional.empty();
    } else {
      try {
        final FileBlock<K> block = getBlockFromFile(blockId);
        final Iterable<SerializedPartition<K>> partitionsInRange = block.readSerializedPartitions(keyRange);
        return Optional.of(partitionsInRange);
      } catch (final IOException e) {
        throw new BlockFetchException(e);
      }
    }
  }

  /**
   * Notifies that all writes for a block is end.
   * Because the block and it's metadata is stored in a remote disk,
   * this store does not have to maintain any information about the block.
   *
   * @param blockId the ID of the block to commit.
   * @return the size of each partition.
   */
  @Override
  public <K extends Serializable>
  Optional<Map<K, Long>> commitBlock(final String blockId) throws BlockWriteException {
    final Block block = blockMap.get(blockId);
    final Optional<Map<K, Long>> partitionSizes;
    if (block != null) {
      try {
        partitionSizes = block.commit();
      } catch (final IOException e) {
        throw new BlockWriteException(e);
      }
    } else {
      throw new BlockWriteException(new Throwable("There isn't any block with id " + blockId));
    }
    blockMap.remove(blockId);
    return partitionSizes;
  }

  /**
   * Removes the file that the target block is stored.
   *
   * @param blockId of the block.
   * @return whether the block exists or not.
   */
  @Override
  public boolean removeBlock(final String blockId) throws BlockFetchException {
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
   * @see FileStore#getFileAreas(String, KeyRange)
   */
  @Override
  public List<FileArea> getFileAreas(final String blockId,
                                     final KeyRange keyRange) {
    final String filePath = DataUtil.blockIdToFilePath(blockId, fileDirectory);

    try {
      if (new File(filePath).isFile()) {
        final FileBlock block = getBlockFromFile(blockId);
        return block.asFileAreas(keyRange);
      } else {
        throw new BlockFetchException(new Throwable(String.format("%s does not exists", blockId)));
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
    return new FileBlock<>(serializer, filePath, metadata);
  }
}
