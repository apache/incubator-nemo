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
package edu.snu.onyx.runtime.executor.data.stores;

import edu.snu.onyx.common.exception.BlockFetchException;
import edu.snu.onyx.conf.JobConf;
import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.common.exception.BlockWriteException;
import edu.snu.onyx.runtime.common.data.KeyRange;
import edu.snu.onyx.runtime.common.message.PersistentConnectionToMasterMap;
import edu.snu.onyx.runtime.executor.data.*;
import edu.snu.onyx.runtime.executor.data.metadata.RemoteFileMetadata;
import edu.snu.onyx.runtime.executor.data.block.FileBlock;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * Stores blocks in a mounted GlusterFS volume.
 * Because the data is stored in remote files and globally accessed by multiple nodes,
 * each access (write, read, or deletion) for a file needs one instance of {@link FileBlock}.
 * These accesses are judiciously synchronized by the metadata server in master.
 * TODO #485: Merge LocalFileStore and GlusterFileStore.
 * TODO #410: Implement metadata caching for the RemoteFileMetadata.
 */
@ThreadSafe
public final class GlusterFileStore extends AbstractBlockStore implements RemoteFileStore {
  private final String fileDirectory;
  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;
  private final String executorId;

  @Inject
  private GlusterFileStore(@Parameter(JobConf.GlusterVolumeDirectory.class) final String volumeDirectory,
                           @Parameter(JobConf.JobId.class) final String jobId,
                           @Parameter(JobConf.ExecutorId.class) final String executorId,
                           final InjectionFuture<BlockManagerWorker> blockManagerWorker,
                           final PersistentConnectionToMasterMap persistentConnectionToMasterMap) {
    super(blockManagerWorker);
    this.fileDirectory = volumeDirectory + "/" + jobId;
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.executorId = executorId;
    new File(fileDirectory).mkdirs();
  }

  /**
   * Creates a new block.
   *
   * @param blockId the ID of the block to create.
   * @see BlockStore#createBlock(String).
   */
  @Override
  public void createBlock(final String blockId) {
    removeBlock(blockId);
  }

  /**
   * Saves an iterable of data partitions to a block.
   *
   * @see BlockStore#putPartitions(String, Iterable, boolean).
   */
  @Override
  public <K extends Serializable> Optional<List<Long>> putPartitions(final String blockId,
                                            final Iterable<NonSerializedPartition<K>> partitions,
                                            final boolean commitPerPartition) throws BlockWriteException {
    try {
      final FileBlock block = createTmpBlock(commitPerPartition, blockId);
      // Serialize and write the given blocks.
      return block.putPartitions(partitions);
    } catch (final IOException e) {
      throw new BlockWriteException(e);
    }
  }

  /**
   * @see BlockStore#putSerializedPartitions(String, Iterable, boolean).
   */
  @Override
  public <K extends Serializable> List<Long> putSerializedPartitions(final String blockId,
                                            final Iterable<SerializedPartition<K>> partitions,
                                            final boolean commitPerPartition) throws BlockWriteException {
    try {
      final FileBlock block = createTmpBlock(commitPerPartition, blockId);
      // Write the given blocks.
      return block.putSerializedPartitions(partitions);
    } catch (final IOException e) {
      throw new BlockWriteException(e);
    }
  }

  /**
   * Retrieves {@link NonSerializedPartition}s in a specific {@link KeyRange} from a block.
   *
   * @see BlockStore#getPartitions(String, KeyRange).
   */
  @Override
  public <K extends Serializable> Optional<Iterable<NonSerializedPartition<K>>> getPartitions(final String blockId,
                                                                  final KeyRange<K> keyRange)
      throws BlockFetchException {
    final String filePath = DataUtil.blockIdToFilePath(blockId, fileDirectory);
    if (!new File(filePath).isFile()) {
      return Optional.empty();
    } else {
      // Deserialize the target data in the corresponding file.
      try {
        final FileBlock block = createTmpBlock(false, blockId);
        final Iterable<NonSerializedPartition<K>> partitionsInRange = block.getPartitions(keyRange);
        return Optional.of(partitionsInRange);
      } catch (final IOException e) {
        throw new BlockFetchException(e);
      }
    }
  }

  /**
   * @see BlockStore#getSerializedPartitions(String, KeyRange).
   */
  @Override
  public <K extends Serializable>
  Optional<Iterable<SerializedPartition<K>>> getSerializedPartitions(final String blockId, final KeyRange<K> keyRange) {
    final String filePath = DataUtil.blockIdToFilePath(blockId, fileDirectory);
    if (!new File(filePath).isFile()) {
      return Optional.empty();
    } else {
      try {
        final FileBlock block = createTmpBlock(false, blockId);
        final Iterable<SerializedPartition<K>> partitionsInRange = block.getSerializedPartitions(keyRange);
        return Optional.of(partitionsInRange);
      } catch (final IOException e) {
        throw new BlockFetchException(e);
      }
    }
  }

  /**
   * @see BlockStore#commitBlock(String).
   */
  @Override
  public void commitBlock(final String blockId) throws BlockWriteException {
    final Coder coder = getCoderFromWorker(blockId);
    final String filePath = DataUtil.blockIdToFilePath(blockId, fileDirectory);

    final RemoteFileMetadata metadata =
        new RemoteFileMetadata(false, blockId, executorId, persistentConnectionToMasterMap);
    new FileBlock(coder, filePath, metadata).commit();
  }

  /**
   * Removes the file that the target block is stored.
   *
   * @param blockId of the block.
   * @return whether the block exists or not.
   */
  @Override
  public Boolean removeBlock(final String blockId) throws BlockFetchException {
    final String filePath = DataUtil.blockIdToFilePath(blockId, fileDirectory);

    try {
      if (new File(filePath).isFile()) {
        final FileBlock block = createTmpBlock(false, blockId);
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
   * @see FileStore#getFileAreas(String, KeyRange).
   */
  @Override
  public List<FileArea> getFileAreas(final String blockId,
                                     final KeyRange keyRange) {
    final String filePath = DataUtil.blockIdToFilePath(blockId, fileDirectory);

    try {
      if (new File(filePath).isFile()) {
        final FileBlock block = createTmpBlock(false, blockId);
        return block.asFileAreas(keyRange);
      } else {
        throw new BlockFetchException(new Throwable(String.format("%s does not exists", blockId)));
      }
    } catch (final IOException e) {
      throw new BlockFetchException(e);
    }
  }

  /**
   * Creates a temporary {@link FileBlock} for a single access.
   * Because the data is stored in remote files and globally accessed by multiple nodes,
   * each access (write, read, or deletion) for a file needs one instance of {@link FileBlock}.
   *
   * @param commitPerPartition whether commit every partition write or not.
   * @param blockId            the ID of the block to create.
   * @return the {@link FileBlock} created.
   */
  private FileBlock createTmpBlock(final boolean commitPerPartition,
                                   final String blockId) {
    final Coder coder = getCoderFromWorker(blockId);
    final String filePath = DataUtil.blockIdToFilePath(blockId, fileDirectory);
    final RemoteFileMetadata metadata =
        new RemoteFileMetadata(commitPerPartition, blockId, executorId, persistentConnectionToMasterMap);
    final FileBlock block = new FileBlock(coder, filePath, metadata);
    return block;
  }
}
