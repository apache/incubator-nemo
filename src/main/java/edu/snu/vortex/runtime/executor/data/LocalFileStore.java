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
package edu.snu.vortex.runtime.executor.data;

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.exception.PartitionFetchException;
import edu.snu.vortex.runtime.exception.PartitionWriteException;
import edu.snu.vortex.runtime.executor.data.partition.LocalFilePartition;
import edu.snu.vortex.runtime.executor.data.partition.MemoryPartition;
import edu.snu.vortex.runtime.executor.data.partition.Partition;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stores partitions in local files.
 * It writes and reads synchronously.
 */
final class LocalFileStore extends FileStore {

  private final Map<String, LocalFilePartition> partitionIdToData;

  @Inject
  private LocalFileStore(@Parameter(JobConf.FileDirectory.class) final String fileDirectory,
                         @Parameter(JobConf.BlockSize.class) final int blockSizeInKb,
                         final InjectionFuture<PartitionManagerWorker> partitionManagerWorker) {
    super(blockSizeInKb, fileDirectory, partitionManagerWorker);
    this.partitionIdToData = new ConcurrentHashMap<>();
    new File(fileDirectory).mkdirs();
  }

  /**
   * Retrieves a deserialized partition of data through disk.
   *
   * @param partitionId of the partition.
   * @return the partition if exist, or an empty optional else.
   * @throws PartitionFetchException if the partition is exist but fail to get the partition.
   */
  @Override
  public Optional<Partition> retrieveDataFromPartition(final String partitionId) throws PartitionFetchException {
    // Deserialize the target data in the corresponding file and pass it as a local data.
    final LocalFilePartition partition = partitionIdToData.get(partitionId);
    if (partition == null) {
      return Optional.empty();
    } else {
      try {
        return Optional.of(new MemoryPartition(partition.asIterable()));
      } catch (final IOException e) {
        throw new PartitionFetchException(e);
      }
    }
  }

  /**
   * @see PartitionStore#retrieveDataFromPartition(String, int, int).
   */
  @Override
  public Optional<Partition> retrieveDataFromPartition(final String partitionId,
                                                       final int hashRangeStartVal,
                                                       final int hashRangeEndVal)
      throws PartitionFetchException {
    // Deserialize the target data in the corresponding file and pass it as a local data.
    final LocalFilePartition partition = partitionIdToData.get(partitionId);
    if (partition == null) {
      return Optional.empty();
    } else {
      try {
        return Optional.of(
            new MemoryPartition(partition.retrieveInHashRange(hashRangeStartVal, hashRangeEndVal)));
      } catch (final IOException e) {
        throw new PartitionFetchException(e);
      }
    }
  }

  /**
   * Saves data in a file as a partition.
   *
   * @param partitionId of the partition.
   * @param data        of to save as a partition.
   * @return the size of the data.
   * @throws PartitionWriteException if fail to put the partition.
   */
  @Override
  public Optional<Long> putDataAsPartition(final String partitionId,
                                           final Iterable<Element> data)
      throws PartitionWriteException {
    final Coder coder = getCoderFromWorker(partitionId);

    try (final LocalFilePartition partition =
             new LocalFilePartition(coder, partitionIdToFileName(partitionId), false)) {
      final Partition previousPartition = partitionIdToData.putIfAbsent(partitionId, partition);
      if (previousPartition != null) {
        throw new PartitionWriteException(new Throwable("Trying to overwrite an existing partition"));
      }

      // Serialize and write the given data into blocks
      partition.openPartitionForWrite();
      final long partitionSize = divideAndPutData(coder, partition, data);
      partition.finishWrite();
      return Optional.of(partitionSize);
    } catch (final IOException e) {
      throw new PartitionWriteException(e);
    }
  }

  /**
   * Saves an iterable of data blocks as a partition.
   * Each block has a specific hash value, and these blocks are sorted by this hash value.
   * The block becomes a unit of read & write.
   *
   * @param partitionId  of the partition.
   * @param sortedData to save as a partition.
   * @return the size of data per hash value.
   * @throws PartitionWriteException thrown for any error occurred while trying to write a partition
   */
  @Override
  public Optional<List<Long>> putSortedDataAsPartition(final String partitionId,
                                                       final Iterable<Iterable<Element>> sortedData)
      throws PartitionWriteException {
    final Coder coder = getCoderFromWorker(partitionId);
    final List<Long> blockSizeList;

    try (final LocalFilePartition partition =
             new LocalFilePartition(coder, partitionIdToFileName(partitionId), true)) {
      final Partition previousPartition = partitionIdToData.putIfAbsent(partitionId, partition);
      if (previousPartition != null) {
        throw new PartitionWriteException(new Throwable("Trying to overwrite an existing partition"));
      }

      // Serialize and write the given data into blocks
      partition.openPartitionForWrite();
      blockSizeList = putSortedData(coder, partition, sortedData);
      partition.finishWrite();
    } catch (final IOException e) {
      throw new PartitionWriteException(e);
    }

    return Optional.of(blockSizeList);
  }

  /**
   * Removes the file that the target partition is stored.
   *
   * @param partitionId of the partition.
   * @return whether the partition exists or not.
   * @throws PartitionFetchException if fail to remove the partition.
   */
  @Override
  public boolean removePartition(final String partitionId) throws PartitionFetchException {
    final LocalFilePartition serializedPartition = partitionIdToData.remove(partitionId);
    if (serializedPartition == null) {
      return false;
    } else {
      try {
        serializedPartition.deleteFile();
      } catch (final IOException e) {
        throw new PartitionFetchException(e);
      }
      return true;
    }
  }
}
