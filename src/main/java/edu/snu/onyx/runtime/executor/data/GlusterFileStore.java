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
package edu.snu.onyx.runtime.executor.data;

import edu.snu.onyx.client.JobConf;
import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.compiler.ir.Element;
import edu.snu.onyx.runtime.exception.PartitionFetchException;
import edu.snu.onyx.runtime.exception.PartitionWriteException;
import edu.snu.onyx.runtime.executor.PersistentConnectionToMasterMap;
import edu.snu.onyx.runtime.executor.data.metadata.RemoteFileMetadata;
import edu.snu.onyx.runtime.executor.data.partition.FilePartition;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.Nullable;
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
 * Concurrent write for a single file is supported, but each writer in different executor
 * has to have separate instance of {@link FilePartition}.
 * These accesses are judiciously synchronized by the metadata server in master.
 * TODO #485: Merge LocalFileStore and GlusterFileStore.
 * TODO #410: Implement metadata caching for the RemoteFileMetadata.
 */
@ThreadSafe
public final class GlusterFileStore extends FileStore implements RemoteFileStore {
  public static final String SIMPLE_NAME = "GlusterFileStore";

  private final PersistentConnectionToMasterMap persistentConnectionToMasterMap;
  private final String executorId;

  @Inject
  private GlusterFileStore(@Parameter(JobConf.GlusterVolumeDirectory.class) final String volumeDirectory,
                           @Parameter(JobConf.JobId.class) final String jobId,
                           @Parameter(JobConf.ExecutorId.class) final String executorId,
                           final InjectionFuture<PartitionManagerWorker> partitionManagerWorker,
                           final PersistentConnectionToMasterMap persistentConnectionToMasterMap) {
    super(volumeDirectory + "/" + jobId, partitionManagerWorker);
    new File(getFileDirectory()).mkdirs();
    this.executorId = executorId;
    this.persistentConnectionToMasterMap = persistentConnectionToMasterMap;
  }

  /**
   * Retrieves a deserialized partition of data through remote disks.
   *
   * @see PartitionStore#getFromPartition(String, HashRange).
   */
  @Override
  public Optional<Iterable<Element>> getFromPartition(final String partitionId,
                                                      final HashRange hashRange) throws PartitionFetchException {
    final String filePath = partitionIdToFilePath(partitionId);
    if (!new File(filePath).isFile()) {
      return Optional.empty();
    } else {
      // Deserialize the target data in the corresponding file.
      final Coder coder = getCoderFromWorker(partitionId);
      FilePartition partition = null;
      try {
        final RemoteFileMetadata metadata =
            new RemoteFileMetadata(false, partitionId, executorId, persistentConnectionToMasterMap);
        partition = new FilePartition(coder, filePath, metadata);
        return Optional.of(partition.retrieveInHashRange(hashRange));
      } catch (final IOException cause) {
        final Throwable combinedThrowable = commitPartitionWithException(partition, cause);
        throw new PartitionFetchException(combinedThrowable);
      }
    }
  }

  /**
   * Saves an iterable of data blocks to a partition.
   *
   * @see PartitionStore#putToPartition(String, Iterable, boolean).
   */
  @Override
  public Optional<List<Long>> putToPartition(final String partitionId,
                                             final Iterable<Block> blocks,
                                             final boolean commitPerBlock) throws PartitionWriteException {
    final Coder coder = getCoderFromWorker(partitionId);
    final String filePath = partitionIdToFilePath(partitionId);
    FilePartition partition = null;

    try {
      final RemoteFileMetadata metadata =
          new RemoteFileMetadata(commitPerBlock, partitionId, executorId, persistentConnectionToMasterMap);
      partition = new FilePartition(coder, filePath, metadata);
      // Serialize and write the given blocks.
      final List<Long> blockSizeList = putBlocks(coder, partition, blocks);
      return Optional.of(blockSizeList);
    } catch (final IOException cause) {
      final Throwable combinedThrowable = commitPartitionWithException(partition, cause);
      throw new PartitionWriteException(combinedThrowable);
    }
  }

  /**
   * @see PartitionStore#commitPartition(String).
   */
  @Override
  public void commitPartition(final String partitionId) throws PartitionWriteException {
    final Coder coder = getCoderFromWorker(partitionId);
    final String filePath = partitionIdToFilePath(partitionId);

    try {
      final RemoteFileMetadata metadata =
          new RemoteFileMetadata(false, partitionId, executorId, persistentConnectionToMasterMap);
      new FilePartition(coder, filePath, metadata).commit();
    } catch (final IOException e) {
      throw new PartitionFetchException(e);
    }
  }

  /**
   * Removes the file that the target partition is stored.
   *
   * @param partitionId of the partition.
   * @return whether the partition exists or not.
   */
  @Override
  public Boolean removePartition(final String partitionId) throws PartitionFetchException {
    final Coder coder = getCoderFromWorker(partitionId);
    final String filePath = partitionIdToFilePath(partitionId);
    FilePartition partition = null;

    try {
      if (new File(filePath).isFile()) {
        final RemoteFileMetadata metadata =
            new RemoteFileMetadata(false, partitionId, executorId, persistentConnectionToMasterMap);
        partition = new FilePartition(coder, filePath, metadata);
        partition.deleteFile();
        return true;
      } else {
        return false;
      }
    } catch (final IOException cause) {
      final Throwable combinedThrowable = commitPartitionWithException(partition, cause);
      throw new PartitionFetchException(combinedThrowable);
    }
  }

  /**
   * @see FileStore#getFileAreas(String, HashRange).
   */
  @Override
  public List<FileArea> getFileAreas(final String partitionId,
                                     final HashRange hashRange) {
    final Coder coder = getCoderFromWorker(partitionId);
    final String filePath = partitionIdToFilePath(partitionId);
    FilePartition partition = null;

    try {
      if (new File(filePath).isFile()) {
        final RemoteFileMetadata metadata =
            new RemoteFileMetadata(false, partitionId, executorId, persistentConnectionToMasterMap);
        partition = new FilePartition(coder, filePath, metadata);
        return partition.asFileAreas(hashRange);
      } else {
        throw new PartitionFetchException(new Throwable(String.format("%s does not exists", partitionId)));
      }
    } catch (final IOException cause) {
      final Throwable combinedThrowable = commitPartitionWithException(partition, cause);
      throw new PartitionFetchException(combinedThrowable);
    }
  }

  /**
   * Commits a partition exceptionally.
   * If there are any subscribers who are waiting the data of the target partition,
   * they will be notified that partition is committed (exceptionally).
   * If failed to commit, it combines the cause and newly thrown exception.
   *
   * @param partition to commit.
   * @param cause     of this exception.
   * @return original cause of this exception if success to commit, combined {@link Throwable} if else.
   */
  private Throwable commitPartitionWithException(@Nullable final FilePartition partition,
                                                 final Throwable cause) {
    try {
      if (partition != null) {
        partition.commit();
      }
    } catch (final IOException closeException) {
      return new Throwable(closeException.getMessage(), cause);
    }
    return cause;
  }
}
