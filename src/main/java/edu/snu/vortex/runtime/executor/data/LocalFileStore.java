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
import edu.snu.vortex.runtime.executor.data.partition.FilePartition;
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
 */
@ThreadSafe
public final class LocalFileStore extends FileStore {
  public static final String SIMPLE_NAME = "LocalFileStore";

  private final Map<String, FilePartition> partitionIdToFilePartition;
  private final ExecutorService executorService;

  @Inject
  private LocalFileStore(@Parameter(JobConf.FileDirectory.class) final String fileDirectory,
                         @Parameter(JobConf.LocalFileStoreNumThreads.class) final int numThreads,
                         final InjectionFuture<PartitionManagerWorker> partitionManagerWorker) {
    super(fileDirectory, partitionManagerWorker);
    this.partitionIdToFilePartition = new ConcurrentHashMap<>();
    new File(fileDirectory).mkdirs();
    executorService = Executors.newFixedThreadPool(numThreads);
  }

  /**
   * Retrieves data in a specific hash range from a partition.
   * @see PartitionStore#getBlocks(String, HashRange).
   */
  @Override
  public Optional<CompletableFuture<Iterable<Element>>> getBlocks(final String partitionId,
                                                                  final HashRange hashRange) {
    // Deserialize the target data in the corresponding file.
    final FilePartition partition = partitionIdToFilePartition.get(partitionId);
    if (partition == null) {
      return Optional.empty();
    } else {
      final Supplier<Iterable<Element>> supplier = () -> {
        try {
          return partition.retrieveInHashRange(hashRange);
        } catch (final IOException retrievalException) {
          final Throwable combinedThrowable = commitPartitionWithException(partitionId, retrievalException);
          throw new PartitionFetchException(combinedThrowable);
        }
      };
      return Optional.of(CompletableFuture.supplyAsync(supplier, executorService));
    }
  }

  /**
   * Saves an iterable of data blocks to a partition.
   * @see PartitionStore#putBlocks(String, Iterable, boolean).
   */
  @Override
  public CompletableFuture<Optional<List<Long>>> putBlocks(final String partitionId,
                                                           final Iterable<Block> blocks,
                                                           final boolean commitPerBlock) {
    final Supplier<Optional<List<Long>>> supplier = () -> {
      final Coder coder = getCoderFromWorker(partitionId);
      final List<Long> blockSizeList;
      final LocalFileMetadata metadata = new LocalFileMetadata(commitPerBlock);

      try {
        FilePartition partition =
            new FilePartition(coder, partitionIdToFilePath(partitionId), metadata);
        partitionIdToFilePartition.putIfAbsent(partitionId, partition);
        partition = partitionIdToFilePartition.get(partitionId);

        // Serialize and write the given blocks.
        blockSizeList = putBlocks(coder, partition, blocks);
      } catch (final IOException writeException) {
        final Throwable combinedThrowable = commitPartitionWithException(partitionId, writeException);
        throw new PartitionWriteException(combinedThrowable);
      }

      return Optional.of(blockSizeList);
    };
    return CompletableFuture.supplyAsync(supplier, executorService);
  }

  /**
   * @see PartitionStore#commitPartition(String).
   */
  @Override
  public void commitPartition(final String partitionId) throws PartitionWriteException {
    final FilePartition partition = partitionIdToFilePartition.get(partitionId);
    if (partition != null) {
      try {
        partition.commit();
      } catch (final IOException e) {
        throw new PartitionWriteException(e);
      }
    } else {
      throw new PartitionWriteException(new Throwable("There isn't any partition with id " + partitionId));
    }
  }

  /**
   * Removes the file that the target partition is stored.
   *
   * @param partitionId of the partition.
   * @return whether the partition exists or not.
   */
  @Override
  public CompletableFuture<Boolean> removePartition(final String partitionId) {
    final FilePartition serializedPartition = partitionIdToFilePartition.remove(partitionId);
    if (serializedPartition == null) {
      return CompletableFuture.completedFuture(false);
    }
    final Supplier<Boolean> supplier = () -> {
      try {
        serializedPartition.deleteFile();
      } catch (final IOException e) {
        final Throwable combinedThrowable = commitPartitionWithException(partitionId, e);
        throw new PartitionFetchException(combinedThrowable);
      }
      return true;
    };
    return CompletableFuture.supplyAsync(supplier, executorService);
  }

  /**
   * @see FileStore#getFileAreas(String, HashRange).
   */
  @Override
  public List<FileArea> getFileAreas(final String partitionId,
                                     final HashRange hashRange) {
    try {
      final FilePartition partition = partitionIdToFilePartition.get(partitionId);
      if (partition == null) {
        throw new IOException(String.format("%s does not exists", partitionId));
      }
      return partition.asFileAreas(hashRange);
    } catch (final IOException retrievalException) {
      final Throwable combinedThrowable = commitPartitionWithException(partitionId, retrievalException);
      throw new PartitionFetchException(combinedThrowable);
    }
  }

  /**
   * Commits a partition exceptionally.
   * If there are any subscribers who are waiting the data of the target partition,
   * they will be notified that partition is committed (exceptionally).
   * If failed to commit, it combines the cause and newly thrown exception.
   *
   * @param partitionId of the partition to commit.
   * @param cause       of this exception.
   * @return original cause of this exception if success to commit, combined {@link Throwable} if else.
   */
  private Throwable commitPartitionWithException(final String partitionId,
                                                 final Throwable cause) {
    try {
      final FilePartition partitionToClose = partitionIdToFilePartition.get(partitionId);
      if (partitionToClose != null) {
        partitionToClose.commit();
      }
    } catch (final IOException closeException) {
      return new Throwable(closeException.getMessage(), cause);
    }
    return cause;
  }
}
