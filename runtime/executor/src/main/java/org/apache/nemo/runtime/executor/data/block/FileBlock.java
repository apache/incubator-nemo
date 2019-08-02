/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.runtime.executor.data.block;

import org.apache.nemo.common.KeyRange;
import org.apache.nemo.runtime.executor.data.MemoryAllocationException;
import org.apache.nemo.runtime.executor.data.MemoryPoolAssigner;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.exception.BlockFetchException;
import org.apache.nemo.common.exception.BlockWriteException;
import org.apache.nemo.runtime.executor.data.DataUtil;
import org.apache.nemo.runtime.executor.data.FileArea;
import org.apache.nemo.runtime.executor.data.metadata.FileMetadata;
import org.apache.nemo.runtime.executor.data.metadata.PartitionMetadata;
import org.apache.nemo.runtime.executor.data.partition.NonSerializedPartition;
import org.apache.nemo.runtime.executor.data.partition.Partition;
import org.apache.nemo.runtime.executor.data.partition.SerializedPartition;
import org.apache.nemo.runtime.executor.data.streamchainer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This class represents a block which is stored in (local or remote) file.
 * Concurrent read is supported, but concurrent write is not supported.
 *
 * @param <K> the key type of its partitions.
 */
@NotThreadSafe
public final class FileBlock<K extends Serializable> implements Block<K> {
  private static final Logger LOG = LoggerFactory.getLogger(FileBlock.class.getName());
  private final String id;
  private final Map<K, SerializedPartition<K>> nonCommittedPartitionsMap;
  private final Serializer serializer;
  private final String filePath;
  private final FileMetadata<K> metadata;
  private final MemoryPoolAssigner memoryPoolAssigner;

  /**
   * Constructor.
   *
   * @param blockId    the ID of this block.
   * @param serializer the {@link Serializer}.
   * @param filePath   the path of the file that this block will be stored.
   * @param metadata   the metadata for this block.
   * @param memoryPoolAssigner  the MemoryPoolAssigner for memory allocation.
   */
  public FileBlock(final String blockId,
                   final Serializer serializer,
                   final String filePath,
                   final FileMetadata<K> metadata,
                   final MemoryPoolAssigner memoryPoolAssigner) {
    this.id = blockId;
    this.nonCommittedPartitionsMap = new HashMap<>();
    this.serializer = serializer;
    this.filePath = filePath;
    this.metadata = metadata;
    this.memoryPoolAssigner = memoryPoolAssigner;
  }

  /**
   * Writes the serialized data of this block having a specific key value as a partition to the file
   * where this block resides.
   * Invariant: This method does not support concurrent write.
   *
   * @param serializedPartitions the iterable of the serialized partitions to write.
   * @throws IOException if fail to write.
   */
  private void writeToFile(final Iterable<SerializedPartition<K>> serializedPartitions)
    throws IOException {
    try (FileChannel fileOutputChannel = new FileOutputStream(filePath, true).getChannel()) {
      for (final SerializedPartition<K> serializedPartition : serializedPartitions) {
        // Reserve a partition write and get the metadata.
        metadata.writePartitionMetadata(serializedPartition.getKey(), serializedPartition.getLength());
        for (final ByteBuffer buffer: serializedPartition.getDirectBufferList()) {
          fileOutputChannel.write(buffer);
        }
        // after the writing to disk, data in memory is released.
        serializedPartition.release();
      }
    }
  }

  /**
   * Writes an element to non-committed block.
   * Invariant: This should not be invoked after this block is committed.
   * Invariant: This method does not support concurrent write.
   *
   * @param key     the key.
   * @param element the element to write.
   * @throws BlockWriteException for any error occurred while trying to write a block.
   */
  @Override
  public void write(final K key,
                    final Object element) throws BlockWriteException {
    if (metadata.isCommitted()) {
      throw new BlockWriteException(new Throwable("The partition is already committed!"));
    } else {
      try {
        SerializedPartition<K> partition = nonCommittedPartitionsMap.get(key);
        if (partition == null) {
          partition = new SerializedPartition<>(key, serializer, memoryPoolAssigner);
          nonCommittedPartitionsMap.put(key, partition);
        }
        partition.write(element);
      } catch (final IOException | MemoryAllocationException e) {
        throw new BlockWriteException(e);
      }
    }
  }

  /**
   * Writes {@link NonSerializedPartition}s to this block.
   * Invariant: This method does not support concurrent write.
   *
   * @param partitions the {@link NonSerializedPartition}s to write.
   * @throws BlockWriteException for any error occurred while trying to write a block.
   */
  @Override
  public void writePartitions(final Iterable<NonSerializedPartition<K>> partitions)
    throws BlockWriteException {
    if (metadata.isCommitted()) {
      throw new BlockWriteException(new Throwable("The partition is already committed!"));
    } else {
      try {
        final Iterable<SerializedPartition<K>> convertedPartitions =
          DataUtil.convertToSerPartitions(serializer, partitions, memoryPoolAssigner);
        writeSerializedPartitions(convertedPartitions);
      } catch (final IOException | MemoryAllocationException e) {
        throw new BlockWriteException(e);
      }
    }
  }

  /**
   * Writes {@link SerializedPartition}s to this block.
   * Invariant: This method does not support concurrent write.
   *
   * @param partitions the {@link SerializedPartition}s to store.
   * @throws BlockWriteException for any error occurred while trying to write a block.
   */
  @Override
  public void writeSerializedPartitions(final Iterable<SerializedPartition<K>> partitions)
    throws BlockWriteException {
    if (metadata.isCommitted()) {
      throw new BlockWriteException(new Throwable("The partition is already committed!"));
    } else {
      try {
        writeToFile(partitions);
      } catch (final IOException e) {
        throw new BlockWriteException(e);
      }
    }
  }

  /**
   * Retrieves the partitions of this block from the file in a specific key range and deserializes it.
   *
   * @param keyRange the key range.
   * @return an iterable of {@link NonSerializedPartition}s.
   * @throws BlockFetchException for any error occurred while trying to fetch a block.
   */
  @Override
  public Iterable<NonSerializedPartition<K>> readPartitions(final KeyRange keyRange) throws BlockFetchException {
    if (!metadata.isCommitted()) {
      throw new BlockFetchException(new Throwable("Cannot retrieve elements before a block is committed"));
    } else {
      // Deserialize the data
      final List<NonSerializedPartition<K>> deserializedPartitions = new ArrayList<>();
      try {
        final List<Pair<K, byte[]>> partitionKeyBytesPairs = new ArrayList<>();
        try (FileInputStream fileStream = new FileInputStream(filePath)) {
          for (final PartitionMetadata<K> partitionMetadata : metadata.getPartitionMetadataList()) {
            final K key = partitionMetadata.getKey();
            if (keyRange.includes(key)) {
              // The key value of this partition is in the range.
              final byte[] partitionBytes = new byte[partitionMetadata.getPartitionSize()];
              fileStream.read(partitionBytes, 0, partitionMetadata.getPartitionSize());
              partitionKeyBytesPairs.add(Pair.of(key, partitionBytes));
            } else {
              // Have to skip this partition.
              skipBytes(fileStream, partitionMetadata.getPartitionSize());
            }
          }
        }
        for (final Pair<K, byte[]> partitionKeyBytes : partitionKeyBytesPairs) {
          final NonSerializedPartition<K> deserializePartition =
            DataUtil.deserializePartition(
              partitionKeyBytes.right().length, serializer, partitionKeyBytes.left(),
              new ByteArrayInputStream(partitionKeyBytes.right()));
          deserializedPartitions.add(deserializePartition);
        }
      } catch (final IOException e) {
        throw new BlockFetchException(e);
      }

      return deserializedPartitions;
    }
  }

  /**
   * Retrieves the {@link SerializedPartition}s in a specific key range.
   * Invariant: This should not be invoked before this block is committed.
   *
   * @param keyRange the key range to retrieve.
   * @return an iterable of {@link SerializedPartition}s.
   * @throws BlockFetchException for any error occurred while trying to fetch a block.
   */
  @Override
  public Iterable<SerializedPartition<K>> readSerializedPartitions(final KeyRange keyRange) throws BlockFetchException {
    if (!metadata.isCommitted()) {
      throw new BlockFetchException(new Throwable("Cannot retrieve elements before a block is committed"));
    } else {
      // Deserialize the data
      final List<SerializedPartition<K>> partitionsInRange = new ArrayList<>();
      try {
        try (FileInputStream fileStream = new FileInputStream(filePath)) {
          for (final PartitionMetadata<K> partitionmetadata : metadata.getPartitionMetadataList()) {
            final K key = partitionmetadata.getKey();
            if (keyRange.includes(key)) {
              // The hash value of this partition is in the range.
              final byte[] serializedData = new byte[partitionmetadata.getPartitionSize()];
              final int readBytes = fileStream.read(serializedData);
              if (readBytes != serializedData.length) {
                throw new IOException("The read data size does not match with the partition size.");
              }
              partitionsInRange.add(new SerializedPartition<>(
                key, serializedData, serializedData.length, memoryPoolAssigner));
            } else {
              // Have to skip this partition.
              skipBytes(fileStream, partitionmetadata.getPartitionSize());
            }
          }
        }
      } catch (final IOException e) {
        throw new BlockFetchException(e);
      }

      return partitionsInRange;
    }
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
    if (!metadata.isCommitted()) {
      throw new IOException("Cannot retrieve elements before a block is committed");
    } else {
      final List<FileArea> fileAreas = new ArrayList<>();
      for (final PartitionMetadata<K> partitionMetadata : metadata.getPartitionMetadataList()) {
        if (keyRange.includes(partitionMetadata.getKey())) {
          fileAreas.add(new FileArea(filePath, partitionMetadata.getOffset(), partitionMetadata.getPartitionSize()));
        }
      }
      return fileAreas;
    }
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
   *
   * @return the size of each partition.
   * @throws BlockWriteException for any error occurred while trying to write a block.
   */
  @Override
  public synchronized Optional<Map<K, Long>> commit() throws BlockWriteException {
    try {
      if (!metadata.isCommitted()) {
        commitPartitions();
        metadata.commitBlock();
      }
      final List<PartitionMetadata<K>> partitionMetadataList = metadata.getPartitionMetadataList();
      final Map<K, Long> partitionSizes = new HashMap<>(partitionMetadataList.size());
      for (final PartitionMetadata<K> partitionMetadata : partitionMetadataList) {
        final K key = partitionMetadata.getKey();
        final long partitionSize = partitionMetadata.getPartitionSize();
        if (partitionSizes.containsKey(key)) {
          partitionSizes.compute(key,
            (existingKey, existingValue) -> existingValue + partitionSize);
        } else {
          partitionSizes.put(key, partitionSize);
        }
      }
      return Optional.of(partitionSizes);
    } catch (final IOException e) {
      throw new BlockWriteException(e);
    }
  }

  /**
   * Commits all un-committed partitions.
   * The committed partitions will be flushed to the storage.
   */
  @Override
  public synchronized void commitPartitions() throws BlockWriteException {
    final List<SerializedPartition<K>> partitions = new ArrayList<>();
    try {
      for (final Partition<?, K> partition : nonCommittedPartitionsMap.values()) {
        partition.commit();
        partitions.add((SerializedPartition<K>) partition);
      }
      writeToFile(partitions);
      nonCommittedPartitionsMap.clear();
    } catch (final IOException e) {
      throw new BlockWriteException(e);
    }
  }

  /**
   * @return the ID of this block.
   */
  @Override
  public String getId() {
    return id;
  }

  /**
   * @return whether this block is committed or not.
   */
  @Override
  public boolean isCommitted() {
    return metadata.isCommitted();
  }
}
