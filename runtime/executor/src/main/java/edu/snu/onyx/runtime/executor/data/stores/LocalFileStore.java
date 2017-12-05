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
import edu.snu.onyx.runtime.common.data.HashRange;
import edu.snu.onyx.runtime.executor.data.*;
import edu.snu.onyx.runtime.executor.data.metadata.LocalFileMetadata;
import edu.snu.onyx.runtime.executor.data.partition.FilePartition;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.*;
import java.util.List;

/**
 * Stores partitions in local files.
 */
@ThreadSafe
public final class LocalFileStore extends LocalPartitionStore implements FileStore {
  private final String fileDirectory;
  private final InjectionFuture<PartitionManagerWorker> partitionManagerWorker;

  @Inject
  private LocalFileStore(@Parameter(JobConf.FileDirectory.class) final String fileDirectory,
                         final InjectionFuture<PartitionManagerWorker> partitionManagerWorker) {
    super(partitionManagerWorker);
    this.fileDirectory = fileDirectory;
    this.partitionManagerWorker = partitionManagerWorker;
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

    final Coder coder = getCoderFromWorker(partitionId);
    final LocalFileMetadata metadata = new LocalFileMetadata(false);

    final FilePartition partition =
        new FilePartition(coder, DataUtil.partitionIdToFilePath(partitionId, fileDirectory), metadata);
    getPartitionMap().put(partitionId, partition);
  }

  /**
   * Removes the file that the target partition is stored.
   *
   * @param partitionId of the partition.
   * @return whether the partition exists or not.
   */
  @Override
  public Boolean removePartition(final String partitionId) throws PartitionFetchException {
    final FilePartition filePartition = (FilePartition) getPartitionMap().remove(partitionId);
    if (filePartition == null) {
      return false;
    }
    try {
      filePartition.deleteFile();
    } catch (final IOException e) {
      throw new PartitionFetchException(e);
    }
    return true;
  }

  /**
   * @see FileStore#getFileAreas(String, HashRange).
   */
  @Override
  public List<FileArea> getFileAreas(final String partitionId,
                                     final HashRange hashRange) {
    try {
      final FilePartition partition = (FilePartition) getPartitionMap().get(partitionId);
      if (partition == null) {
        throw new IOException(String.format("%s does not exists", partitionId));
      }
      return partition.asFileAreas(hashRange);
    } catch (final IOException retrievalException) {
      throw new PartitionFetchException(retrievalException);
    }
  }
}
