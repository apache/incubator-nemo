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
package edu.snu.nemo.runtime.executor.data.block;

import edu.snu.nemo.runtime.common.data.KeyRange;
import edu.snu.nemo.runtime.executor.data.*;
import edu.snu.nemo.runtime.executor.data.streamchainer.Serializer;
import edu.snu.nemo.runtime.executor.data.metadata.PartitionMetadata;
import edu.snu.nemo.runtime.executor.data.metadata.FileMetadata;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * This class represents a block which is stored in (local or remote) file.
 * @param <K> the key type of its partitions.
 */
public final class FileBlock<K extends Serializable> implements Block<K> {

  private final Serializer serializer;
  private final String filePath;
  private final FileMetadata<K> metadata;

  /**
   * Constructor.
   *
   * @param serializer    the {@link Serializer}.
   * @param filePath the path of the file that this block will be stored.
   * @param metadata the metadata for this block.
   */
  public FileBlock(final Serializer serializer,
                   final String filePath,
                   final FileMetadata<K> metadata) {
    this.serializer = serializer;
    this.filePath = filePath;
    this.metadata = metadata;
  }

  /**
   * Writes the serialized data of this block having a specific key value as a partition to the file
   * where this block resides.
   * Invariant: This method does not support concurrent write for a single block.
   *            Only one thread have to write at once.
   *
   * @param serializedPartitions the iterable of the serialized partitions to write.
   * @throws IOException if fail to write.
   */
  private void writeSerializedPartitions(final Iterable<SerializedPartition<K>> serializedPartitions)
      throws IOException {
    try (final FileOutputStream fileOutputStream = new FileOutputStream(filePath, true)) {
      for (final SerializedPartition<K> serializedPartition : serializedPartitions) {
        // Reserve a partition write and get the metadata.
        metadata.writePartitionMetadata(
            serializedPartition.getKey(), serializedPartition.getLength(), serializedPartition.getElementsTotal());
        fileOutputStream.write(serializedPartition.getData(), 0, serializedPartition.getLength());
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
  public Optional<List<Long>> putPartitions(final Iterable<NonSerializedPartition<K>> partitions) throws IOException {
    final Iterable<SerializedPartition<K>> convertedPartitions =
        DataUtil.convertToSerPartitions(serializer, partitions);

    return Optional.of(putSerializedPartitions(convertedPartitions));
  }

  /**
   * Writes {@link SerializedPartition}s to this block.
   *
   * @param partitions the {@link SerializedPartition}s to store.
   * @throws IOException if fail to store.
   */
  @Override
  public synchronized List<Long> putSerializedPartitions(final Iterable<SerializedPartition<K>> partitions)
      throws IOException {
    final List<Long> partitionSizeList = new ArrayList<>();
    for (final SerializedPartition serializedPartition : partitions) {
      partitionSizeList.add((long) serializedPartition.getLength());
    }
    writeSerializedPartitions(partitions);

    return partitionSizeList;
  }

  /**
   * Retrieves the partitions of this block from the file in a specific key range and deserializes it.
   *
   * @param keyRange the key range.
   * @return an iterable of {@link NonSerializedPartition}s.
   * @throws IOException if failed to retrieve.
   */
  @Override
  public Iterable<NonSerializedPartition<K>> getPartitions(final KeyRange keyRange) throws IOException {
    // Deserialize the data
    final List<NonSerializedPartition<K>> deserializedPartitions = new ArrayList<>();
    try (final FileInputStream fileStream = new FileInputStream(filePath)) {
      for (final PartitionMetadata<K> partitionMetadata : metadata.getPartitionMetadataIterable()) {
        final K key = partitionMetadata.getKey();
        if (keyRange.includes(key)) {
          // The key value of this partition is in the range.
          final long availableBefore = fileStream.available();
          // We need to limit read bytes on this FileStream, which could be over-read by wrapped
          // compression stream. We recommend to wrap with LimitedInputStream once more when
          // reading input from chained compression InputStream.
          // Plus, this stream must be not closed to prevent to close the filtered file partition.
          final LimitedInputStream limitedInputStream =
              new LimitedInputStream(fileStream, partitionMetadata.getPartitionSize());
          final NonSerializedPartition<K> deserializePartition =
              DataUtil.deserializePartition(
                  partitionMetadata.getElementsTotal(), serializer, key, limitedInputStream);
          deserializedPartitions.add(deserializePartition);
          // rearrange file pointer
          final long toSkip = partitionMetadata.getPartitionSize() - availableBefore + fileStream.available();
          if (toSkip > 0) {
            skipBytes(fileStream, toSkip);
          } else if (toSkip < 0) {
            throw new IOException("file stream has been overread");
          }
        } else {
          // Have to skip this partition.
          skipBytes(fileStream, partitionMetadata.getPartitionSize());
        }
      }
    }

    return deserializedPartitions;
  }

  /**
   * Retrieves the {@link SerializedPartition}s in a specific key range.
   * Invariant: This should not be invoked before this block is committed.
   *
   * @param keyRange the key range to retrieve.
   * @return an iterable of {@link SerializedPartition}s.
   * @throws IOException if failed to retrieve.
   */
  @Override
  public Iterable<SerializedPartition<K>> getSerializedPartitions(final KeyRange keyRange) throws IOException {
    // Deserialize the data
    final List<SerializedPartition<K>> partitionsInRange = new ArrayList<>();
    try (final FileInputStream fileStream = new FileInputStream(filePath)) {
      for (final PartitionMetadata<K> partitionmetadata : metadata.getPartitionMetadataIterable()) {
        final K key = partitionmetadata.getKey();
        if (keyRange.includes(key)) {
          // The hash value of this partition is in the range.
          final byte[] serializedData = new byte[partitionmetadata.getPartitionSize()];
          final int readBytes = fileStream.read(serializedData);
          if (readBytes != serializedData.length) {
            throw new IOException("The read data size does not match with the partition size.");
          }
          partitionsInRange.add(new SerializedPartition<>(
              key, partitionmetadata.getElementsTotal(), serializedData, serializedData.length));
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
   * Retrieves the list of {@link FileArea}s for the specified {@link KeyRange}.
   *
   * @param keyRange the key range
   * @return list of the file areas
   * @throws IOException if failed to open a file channel
   */
  public List<FileArea> asFileAreas(final KeyRange keyRange) throws IOException {
    final List<FileArea> fileAreas = new ArrayList<>();
    for (final PartitionMetadata<K> partitionMetadata : metadata.getPartitionMetadataIterable()) {
      if (keyRange.includes(partitionMetadata.getKey())) {
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
    if (new File(filePath).exists()) {
      Files.delete(Paths.get(filePath));
    }
  }

  /**
   * Commits this block to prevent further write.
   */
  @Override
  public void commit() throws IOException {
    metadata.commitBlock();
  }
}
