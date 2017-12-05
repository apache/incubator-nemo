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

import edu.snu.onyx.conf.JobConf;
import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.common.exception.PartitionFetchException;
import edu.snu.onyx.common.exception.PartitionWriteException;
import edu.snu.onyx.runtime.common.data.HashRange;
import edu.snu.onyx.runtime.common.message.PersistentConnectionToMasterMap;
import edu.snu.onyx.runtime.executor.data.*;
import edu.snu.onyx.runtime.executor.data.metadata.RemoteFileMetadata;
import edu.snu.onyx.runtime.executor.data.partition.FilePartition;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Stores partitions in a mounted GlusterFS volume.
 * Because the data is stored in remote files and globally accessed by multiple nodes,
 * each access (write, read, or deletion) for a file needs one instance of {@link FilePartition}.
 * These accesses are judiciously synchronized by the metadata server in master.
 * TODO #485: Merge LocalFileStore and GlusterFileStore.
 * TODO #410: Implement metadata caching for the RemoteFileMetadata.
 */
@ThreadSafe
public final class GlusterFileStore extends AbstractPartitionStore implements RemoteFileStore {
  private final String fileDirectory;
  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;
  private final String executorId;

  @Inject
  private GlusterFileStore(@Parameter(JobConf.GlusterVolumeDirectory.class) final String volumeDirectory,
                           @Parameter(JobConf.JobId.class) final String jobId,
                           @Parameter(JobConf.ExecutorId.class) final String executorId,
                           final InjectionFuture<PartitionManagerWorker> partitionManagerWorker,
                           final PersistentConnectionToMasterMap persistentConnectionToMasterMap) {
    super(partitionManagerWorker);
    this.fileDirectory = volumeDirectory + "/" + jobId;
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
    this.executorId = executorId;
    new File(fileDirectory).mkdirs();
  }

  /**
   * Creates a new partition.
   *
   * @param partitionId the ID of the partition to create.
   * @see PartitionStore#createPartition(String).
   */
  @Override
  public void createPartition(final String partitionId) {
    removePartition(partitionId);
  }

  /**
   * Saves an iterable of data blocks to a partition.
   *
   * @see PartitionStore#putBlocks(String, Iterable, boolean).
   */
  @Override
  public Optional<List<Long>> putBlocks(final String partitionId,
                                        final Iterable<NonSerializedBlock> blocks,
                                        final boolean commitPerBlock) throws PartitionWriteException {
    try {
      final FilePartition partition = createTmpPartition(commitPerBlock, partitionId);
      // Serialize and write the given blocks.
      return partition.putBlocks(blocks);
    } catch (final IOException e) {
      throw new PartitionWriteException(e);
    }
  }

  /**
   * @see PartitionStore#putSerializedBlocks(String, Iterable, boolean).
   */
  @Override
  public List<Long> putSerializedBlocks(final String partitionId,
                                        final Iterable<SerializedBlock> blocks,
                                        final boolean commitPerBlock) throws PartitionWriteException {
    try {
      final FilePartition partition = createTmpPartition(commitPerBlock, partitionId);
      // Write the given blocks.
      return partition.putSerializedBlocks(blocks);
    } catch (final IOException e) {
      throw new PartitionWriteException(e);
    }
  }

  /**
   * Retrieves a deserialized partition of elements through remote disks.
   *
   * @see PartitionStore#getBlocks(String, HashRange).
   */
  @Override
  public Optional<Iterable<NonSerializedBlock>> getBlocks(final String partitionId,
                                                          final HashRange hashRange) throws PartitionFetchException {
    final String filePath = DataUtil.partitionIdToFilePath(partitionId, fileDirectory);
    if (!new File(filePath).isFile()) {
      return Optional.empty();
    } else {
      // Deserialize the target data in the corresponding file.
      try {
        final FilePartition partition = createTmpPartition(false, partitionId);
        final Iterable<NonSerializedBlock> blocksInRange = partition.getBlocks(hashRange);
        return Optional.of(blocksInRange);
      } catch (final IOException e) {
        throw new PartitionFetchException(e);
      }
    }
  }

  /**
   * @see PartitionStore#getSerializedBlocks(String, HashRange).
   */
  @Override
  public Optional<Iterable<SerializedBlock>> getSerializedBlocks(final String partitionId,
                                                                 final HashRange hashRange) {
    final String filePath = DataUtil.partitionIdToFilePath(partitionId, fileDirectory);
    if (!new File(filePath).isFile()) {
      return Optional.empty();
    } else {
      try {
        final FilePartition partition = createTmpPartition(false, partitionId);
        final Iterable<SerializedBlock> blocksInRange = partition.getSerializedBlocks(hashRange);
        return Optional.of(blocksInRange);
      } catch (final IOException e) {
        throw new PartitionFetchException(e);
      }
    }
  }

  /**
   * @see PartitionStore#commitPartition(String).
   */
  @Override
  public void commitPartition(final String partitionId) throws PartitionWriteException {
    final Coder coder = getCoderFromWorker(partitionId);
    final String filePath = DataUtil.partitionIdToFilePath(partitionId, fileDirectory);

    final RemoteFileMetadata metadata =
        new RemoteFileMetadata(false, partitionId, executorId, persistentConnectionToMasterMap);
    new FilePartition(coder, filePath, metadata).commit();
  }

  /**
   * Removes the file that the target partition is stored.
   *
   * @param partitionId of the partition.
   * @return whether the partition exists or not.
   */
  @Override
  public Boolean removePartition(final String partitionId) throws PartitionFetchException {
    final String filePath = DataUtil.partitionIdToFilePath(partitionId, fileDirectory);

    try {
      if (new File(filePath).isFile()) {
        final FilePartition partition = createTmpPartition(false, partitionId);
        partition.deleteFile();
        return true;
      } else {
        return false;
      }
    } catch (final IOException e) {
      throw new PartitionFetchException(e);
    }
  }

  /**
   * @see FileStore#getFileAreas(String, HashRange).
   */
  @Override
  public List<FileArea> getFileAreas(final String partitionId,
                                     final HashRange hashRange) {
    final String filePath = DataUtil.partitionIdToFilePath(partitionId, fileDirectory);

    try {
      if (new File(filePath).isFile()) {
        final FilePartition partition = createTmpPartition(false, partitionId);
        return partition.asFileAreas(hashRange);
      } else {
        throw new PartitionFetchException(new Throwable(String.format("%s does not exists", partitionId)));
      }
    } catch (final IOException e) {
      throw new PartitionFetchException(e);
    }
  }

  /**
   * Creates a temporary {@link FilePartition} for a single access.
   * Because the data is stored in remote files and globally accessed by multiple nodes,
   * each access (write, read, or deletion) for a file needs one instance of {@link FilePartition}.
   *
   * @param commitPerBlock whether commit every block write or not.
   * @param partitionId    the ID of the partition to create.
   * @return the {@link FilePartition} created.
   */
  private FilePartition createTmpPartition(final boolean commitPerBlock,
                                           final String partitionId) {
    final Coder coder = getCoderFromWorker(partitionId);
    final String filePath = DataUtil.partitionIdToFilePath(partitionId, fileDirectory);
    final RemoteFileMetadata metadata =
        new RemoteFileMetadata(commitPerBlock, partitionId, executorId, persistentConnectionToMasterMap);
    final FilePartition partition = new FilePartition(coder, filePath, metadata);
    return partition;
  }
}
