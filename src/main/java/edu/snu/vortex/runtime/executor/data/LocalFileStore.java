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
import edu.snu.vortex.runtime.executor.data.metadata.LocalFileMetadata;
import edu.snu.vortex.runtime.executor.data.partition.LocalFilePartition;
import edu.snu.vortex.runtime.executor.data.partition.MemoryPartition;
import edu.snu.vortex.runtime.executor.data.partition.Partition;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * Stores partitions in local files.
 * It writes and reads synchronously.
 */
@ThreadSafe
final class LocalFileStore extends FileStore {

  private final Map<String, LocalFilePartition> partitionIdToData;

  private final ExecutorService executorService;

  @Inject
  private LocalFileStore(@Parameter(JobConf.FileDirectory.class) final String fileDirectory,
                         @Parameter(JobConf.BlockSize.class) final int blockSizeInKb,
                         @Parameter(JobConf.LocalFileStoreNumThreads.class) final int numThreads,
                         final InjectionFuture<PartitionManagerWorker> partitionManagerWorker) {
    super(blockSizeInKb, fileDirectory, partitionManagerWorker);
    this.partitionIdToData = new ConcurrentHashMap<>();
    new File(fileDirectory).mkdirs();
    executorService = Executors.newFixedThreadPool(numThreads);
  }

  /**
   * Retrieves a deserialized partition of data through disk.
   *
   * @param partitionId of the partition.
   * @return the partition if exist, or an empty optional else.
   */
  @Override
  public CompletableFuture<Optional<Partition>> retrieveDataFromPartition(final String partitionId) {
    final LocalFilePartition partition = partitionIdToData.get(partitionId);
    if (partition == null) {
      return CompletableFuture.completedFuture(Optional.empty());
    }
    // Deserialize the target data in the corresponding file and pass it as a local data.
    final Supplier<Optional<Partition>> supplier = () -> {
      try {
        return Optional.of(new MemoryPartition(partition.asIterable()));
      } catch (final IOException e) {
        throw new PartitionFetchException(e);
      }
    };
    return CompletableFuture.supplyAsync(supplier, executorService);
  }

  /**
   * @see PartitionStore#retrieveDataFromPartition(String, int, int).
   */
  @Override
  public CompletableFuture<Optional<Partition>> retrieveDataFromPartition(final String partitionId,
                                                                          final int hashRangeStartVal,
                                                                          final int hashRangeEndVal) {
    // Deserialize the target data in the corresponding file and pass it as a local data.
    final LocalFilePartition partition = partitionIdToData.get(partitionId);
    if (partition == null) {
      return CompletableFuture.completedFuture(Optional.empty());
    }
    final Supplier<Optional<Partition>> supplier = () -> {
      try {
        return Optional.of(
            new MemoryPartition(partition.retrieveInHashRange(hashRangeStartVal, hashRangeEndVal)));
      } catch (final IOException e) {
        throw new PartitionFetchException(e);
      }
    };
    return CompletableFuture.supplyAsync(supplier, executorService);
  }

  /**
   * Saves data in a file as a partition.
   *
   * @param partitionId of the partition.
   * @param data        of to save as a partition.
   * @return the size of the data.
   */
  @Override
  public CompletableFuture<Optional<Long>> putDataAsPartition(final String partitionId,
                                                              final Iterable<Element> data) {
    final Supplier<Optional<Long>> supplier = () -> {
      final Coder coder = getCoderFromWorker(partitionId);
      final LocalFileMetadata metadata = new LocalFileMetadata(false);

      try (final LocalFilePartition partition =
               new LocalFilePartition(coder, partitionIdToFilePath(partitionId), metadata)) {
        final Partition previousPartition = partitionIdToData.putIfAbsent(partitionId, partition);
        if (previousPartition != null) {
          throw new PartitionWriteException(new Throwable("Trying to overwrite an existing partition"));
        }

        // Serialize and write the given data into blocks
        final long partitionSize = divideAndPutData(coder, partition, data);
        return Optional.of(partitionSize);
      } catch (final IOException e) {
        throw new PartitionWriteException(e);
      }
    };
    return CompletableFuture.supplyAsync(supplier, executorService);
  }

  /**
   * Saves an iterable of data blocks as a partition.
   * Each block has a specific hash value, and the block becomes a unit of read & write.
   *
   * @param partitionId  of the partition.
   * @param hashedData to save as a partition.
   * @return the size of data per hash value.
   */
  @Override
  public CompletableFuture<Optional<List<Long>>> putHashedDataAsPartition(
      final String partitionId, final Iterable<Iterable<Element>> hashedData) {
    final Supplier<Optional<List<Long>>> supplier = () -> {
      final Coder coder = getCoderFromWorker(partitionId);
      final List<Long> blockSizeList;
      final LocalFileMetadata metadata = new LocalFileMetadata(true);

      try (final LocalFilePartition partition =
               new LocalFilePartition(coder, partitionIdToFilePath(partitionId), metadata)) {
        final Partition previousPartition = partitionIdToData.putIfAbsent(partitionId, partition);
        if (previousPartition != null) {
          throw new PartitionWriteException(new Throwable("Trying to overwrite an existing partition"));
        }

        // Serialize and write the given data into blocks
        blockSizeList = putHashedData(coder, partition, hashedData);
      } catch (final IOException e) {
        throw new PartitionWriteException(e);
      }

      return Optional.of(blockSizeList);
    };
    return CompletableFuture.supplyAsync(supplier, executorService);
  }

  /**
   * Removes the file that the target partition is stored.
   *
   * @param partitionId of the partition.
   * @return whether the partition exists or not.
   */
  @Override
  public CompletableFuture<Boolean> removePartition(final String partitionId) {
    final LocalFilePartition serializedPartition = partitionIdToData.remove(partitionId);
    if (serializedPartition == null) {
      return CompletableFuture.completedFuture(false);
    }
    final Supplier<Boolean> supplier = () -> {
      try {
        serializedPartition.deleteFile();
      } catch (final IOException e) {
        throw new PartitionFetchException(e);
      }
      return true;
    };
    return CompletableFuture.supplyAsync(supplier, executorService);
  }
}
