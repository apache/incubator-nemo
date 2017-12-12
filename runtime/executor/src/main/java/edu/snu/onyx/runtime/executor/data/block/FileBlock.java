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
package edu.snu.onyx.runtime.executor.data.block;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.runtime.executor.data.*;
import edu.snu.onyx.runtime.common.data.HashRange;
import edu.snu.onyx.runtime.executor.data.metadata.PartitionMetadata;
import edu.snu.onyx.runtime.executor.data.metadata.FileMetadata;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class represents a block which is stored in (local or remote) file.
 */
public final class FileBlock implements Block {

  private final Coder coder;
  private final String filePath;
  private final FileMetadata metadata;
  private final Queue<PartitionMetadata> partitionMetadataToCommit;
  private final boolean commitPerBlock;

  public FileBlock(final Coder coder,
                   final String filePath,
                   final FileMetadata metadata) {
    this.coder = coder;
    this.filePath = filePath;
    this.metadata = metadata;
    this.partitionMetadataToCommit = new ConcurrentLinkedQueue<>();
    this.commitPerBlock = metadata.isPartitionCommitPerWrite();
  }

  /**
   * Writes the serialized data of this block having a specific hash value as a partition to the file
   * where this block resides.
   * Invariant: This method does not support concurrent write for a single block.
   *            Only one thread have to write at once.
   *
   * @param serializedPartitions the iterable of the serialized partitions to write.
   * @throws IOException if fail to write.
   */
  private void writeSerializedPartitions(final Iterable<SerializedPartition> serializedPartitions) throws IOException {
    try (final FileOutputStream fileOutputStream = new FileOutputStream(filePath, true)) {
      for (final SerializedPartition serializedPartition : serializedPartitions) {
        // Reserve a partition write and get the metadata.
        final PartitionMetadata partitionMetadata = metadata.reservePartition(
            serializedPartition.getKey(), serializedPartition.getLength(), serializedPartition.getElementsTotal());
        fileOutputStream.write(serializedPartition.getData(), 0, serializedPartition.getLength());

        // Commit if needed.
        if (commitPerBlock) {
          metadata.commitPartitions(Collections.singleton(partitionMetadata));
        } else {
          partitionMetadataToCommit.add(partitionMetadata);
        }
      }
    }
  }

  /**
   * Writes {@link NonSerializedPartition}s to this block.
   *
   * @param partitions the {@link NonSerializedPartition}s to write.
   * @throws IOException if fail to write.
   */
  @Override
  public Optional<List<Long>> putPartitions(final Iterable<NonSerializedPartition> partitions) throws IOException {
    final Iterable<SerializedPartition> convertedPartitions = DataUtil.convertToSerPartitions(coder, partitions);

    return Optional.of(putSerializedPartitions(convertedPartitions));
  }


  /**
   * Writes {@link SerializedPartition}s to this block.
   *
   * @param partitions the {@link SerializedPartition}s to store.
   * @throws IOException if fail to store.
   */
  @Override
  public synchronized List<Long> putSerializedPartitions(final Iterable<SerializedPartition> partitions)
      throws IOException {
    final List<Long> partitionSizeList = new ArrayList<>();
    for (final SerializedPartition serializedPartition : partitions) {
      partitionSizeList.add((long) serializedPartition.getLength());
    }
    writeSerializedPartitions(partitions);
    commitRemainderMetadata();

    return partitionSizeList;
  }

  /**
   * Commits the un-committed partition metadata.
   */
  private void commitRemainderMetadata() {
    final List<PartitionMetadata> metadataToCommit = new ArrayList<>();
    while (!partitionMetadataToCommit.isEmpty()) {
      final PartitionMetadata partitionMetadata = partitionMetadataToCommit.poll();
      if (partitionMetadata != null) {
        metadataToCommit.add(partitionMetadata);
      }
    }
    metadata.commitPartitions(metadataToCommit);
  }

  /**
   * Retrieves the partitions of this block from the file in a specific hash range and deserializes it.
   *
   * @param hashRange the hash range.
   * @return an iterable of {@link NonSerializedPartition}s.
   * @throws IOException if failed to retrieve.
   */
  @Override
  public Iterable<NonSerializedPartition> getPartitions(final HashRange hashRange) throws IOException {
    // Deserialize the data
    final List<NonSerializedPartition> deserializedPartitions = new ArrayList<>();
    try (final FileInputStream fileStream = new FileInputStream(filePath)) {
      for (final PartitionMetadata partitionMetadata : metadata.getPartitionMetadataIterable()) {
        final int hashVal = partitionMetadata.getHashValue();
        if (hashRange.includes(hashVal)) {
          // The hash value of this partition is in the range.
          final NonSerializedPartition deserializePartition =
              DataUtil.deserializePartition(partitionMetadata.getElementsTotal(), coder, hashVal, fileStream);
          deserializedPartitions.add(deserializePartition);
        } else {
          // Have to skip this partition.
          skipBytes(fileStream, partitionMetadata.getPartitionSize());
        }
      }
    }

    return deserializedPartitions;
  }

  /**
   * Retrieves the {@link SerializedPartition}s in a specific hash range.
   * Invariant: This should not be invoked before this block is committed.
   *
   * @param hashRange the hash range to retrieve.
   * @return an iterable of {@link SerializedPartition}s.
   * @throws IOException if failed to retrieve.
   */
  @Override
  public Iterable<SerializedPartition> getSerializedPartitions(final HashRange hashRange) throws IOException {
    // Deserialize the data
    final List<SerializedPartition> partitionsInRange = new ArrayList<>();
    try (final FileInputStream fileStream = new FileInputStream(filePath)) {
      for (final PartitionMetadata partitionmetadata : metadata.getPartitionMetadataIterable()) {
        final int hashVal = partitionmetadata.getHashValue();
        if (hashRange.includes(hashVal)) {
          // The hash value of this partition is in the range.
          final byte[] serializedData = new byte[partitionmetadata.getPartitionSize()];
          final int readBytes = fileStream.read(serializedData);
          if (readBytes != serializedData.length) {
            throw new IOException("The read data size does not match with the partition size.");
          }
          partitionsInRange.add(new SerializedPartition(
              hashVal, partitionmetadata.getElementsTotal(), serializedData, serializedData.length));
        } else {
          // Have to skip this partition.
          skipBytes(fileStream, partitionmetadata.getPartitionSize());
        }
      }
    }

    return partitionsInRange;
  }

  /**
   * Skips some bytes in a input stream.
   *
   * @param inputStream the stream to skip.
   * @param bytesToSkip the number of bytes to skip.
   * @throws IOException if fail to skip.
   */
  private void skipBytes(final InputStream inputStream,
                         final long bytesToSkip) throws IOException {
    long remainingBytesToSkip = bytesToSkip;
    while (remainingBytesToSkip > 0) {
      final long skippedBytes = inputStream.skip(bytesToSkip);
      remainingBytesToSkip -= skippedBytes;
      if (skippedBytes <= 0) {
        throw new IOException("The file stream failed to skip to the next block.");
      }
    }
  }

  /**
   * Retrieves the list of {@link FileArea}s for the specified {@link HashRange}.
   *
   * @param hashRange the hash range
   * @return list of the file areas
   * @throws IOException if failed to open a file channel
   */
  public List<FileArea> asFileAreas(final HashRange hashRange) throws IOException {
    final List<FileArea> fileAreas = new ArrayList<>();
    for (final PartitionMetadata partitionMetadata : metadata.getPartitionMetadataIterable()) {
      if (hashRange.includes(partitionMetadata.getHashValue())) {
        fileAreas.add(new FileArea(filePath, partitionMetadata.getOffset(), partitionMetadata.getPartitionSize()));
      }
    }
    return fileAreas;
  }

  /**
   * Deletes the file that contains this block data.
   * This method have to be called after all read is completed (or failed).
   *
   * @throws IOException if failed to delete.
   */
  public void deleteFile() throws IOException {
    metadata.deleteMetadata();
    Files.delete(Paths.get(filePath));
  }

  /**
   * Commits this block to prevent further write.
   */
  @Override
  public void commit() {
    commitRemainderMetadata();
    metadata.commitBlock();
  }
}
