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
package edu.snu.onyx.runtime.executor.data.partition;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.runtime.executor.data.*;
import edu.snu.onyx.runtime.common.data.HashRange;
import edu.snu.onyx.runtime.executor.data.metadata.BlockMetadata;
import edu.snu.onyx.runtime.executor.data.metadata.FileMetadata;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class represents a partition which is stored in (local or remote) file.
 */
public final class FilePartition implements Partition {

  private final Coder coder;
  private final String filePath;
  private final FileMetadata metadata;
  private final Queue<BlockMetadata> blockMetadataToCommit;
  private final boolean commitPerBlock;

  public FilePartition(final Coder coder,
                       final String filePath,
                       final FileMetadata metadata) {
    this.coder = coder;
    this.filePath = filePath;
    this.metadata = metadata;
    this.blockMetadataToCommit = new ConcurrentLinkedQueue<>();
    this.commitPerBlock = metadata.isBlockCommitPerWrite();
  }

  /**
   * Writes the serialized data of this partition having a specific hash value as a block to the file
   * where this partition resides.
   * Invariant: This method not support concurrent write for a single partition.
   *            Only one thread have to write at once.
   *
   * @param serializedBlocks the iterable of the serialized blocks to write.
   * @throws IOException if fail to write.
   */
  private void writeSerializedBlocks(final Iterable<SerializedBlock> serializedBlocks) throws IOException {
    try (final FileOutputStream fileOutputStream = new FileOutputStream(filePath, true)) {
      for (final SerializedBlock serializedBlock : serializedBlocks) {
        // Reserve a block write and get the metadata.
        final BlockMetadata blockMetadata = metadata.reserveBlock(
            serializedBlock.getKey(), serializedBlock.getData().length, serializedBlock.getElementsTotal());
        fileOutputStream.write(serializedBlock.getData());

        // Commit if needed.
        if (commitPerBlock) {
          metadata.commitBlocks(Collections.singleton(blockMetadata));
        } else {
          blockMetadataToCommit.add(blockMetadata);
        }
      }
    }
  }

  /**
   * Writes {@link NonSerializedBlock}s to this partition.
   *
   * @param blocksToStore the {@link NonSerializedBlock}s to write.
   * @throws IOException if fail to write.
   */
  @Override
  public Optional<List<Long>> putBlocks(final Iterable<NonSerializedBlock> blocksToStore) throws IOException {
    final Iterable<SerializedBlock> convertedBlocks = DataUtil.convertToSerBlocks(coder, blocksToStore);

    return Optional.of(putSerializedBlocks(convertedBlocks));
  }


  /**
   * Writes {@link SerializedBlock}s to this partition.
   *
   * @param blocksToStore the {@link SerializedBlock}s to store.
   * @throws IOException if fail to store.
   */
  @Override
  public synchronized List<Long> putSerializedBlocks(final Iterable<SerializedBlock> blocksToStore)
      throws IOException {
    final List<Long> blockSizeList = new ArrayList<>();
    for (final SerializedBlock convertedBlock : blocksToStore) {
      blockSizeList.add((long) convertedBlock.getData().length);
    }
    writeSerializedBlocks(blocksToStore);
    commitRemainderMetadata();

    return blockSizeList;
  }

  /**
   * Commits the un-committed block metadata.
   */
  private void commitRemainderMetadata() {
    final List<BlockMetadata> metadataToCommit = new ArrayList<>();
    while (!blockMetadataToCommit.isEmpty()) {
      final BlockMetadata blockMetadata = blockMetadataToCommit.poll();
      if (blockMetadata != null) {
        metadataToCommit.add(blockMetadata);
      }
    }
    metadata.commitBlocks(metadataToCommit);
  }

  /**
   * Retrieves the blocks of this partition from the file in a specific hash range and deserializes it.
   *
   * @param hashRange the hash range
   * @return an iterable of {@link NonSerializedBlock}s.
   * @throws IOException if failed to retrieve.
   */
  @Override
  public Iterable<NonSerializedBlock> getBlocks(final HashRange hashRange) throws IOException {
    // Deserialize the data
    final List<NonSerializedBlock> deserializedBlocks = new ArrayList<>();
    try (final FileInputStream fileStream = new FileInputStream(filePath)) {
      for (final BlockMetadata blockMetadata : metadata.getBlockMetadataIterable()) {
        final int hashVal = blockMetadata.getHashValue();
        if (hashRange.includes(hashVal)) {
          // The hash value of this block is in the range.
          final NonSerializedBlock deserializeBlock =
              DataUtil.deserializeBlock(blockMetadata.getElementsTotal(), coder, hashVal, fileStream);
          deserializedBlocks.add(deserializeBlock);
        } else {
          // Have to skip this block.
          skipBytes(fileStream, blockMetadata.getBlockSize());
        }
      }
    }

    return deserializedBlocks;
  }

  /**
   * Retrieves the {@link SerializedBlock}s in a specific hash range.
   * Invariant: This should not be invoked before this partition is committed.
   *
   * @param hashRange the hash range to retrieve.
   * @return an iterable of {@link SerializedBlock}s.
   * @throws IOException if failed to retrieve.
   */
  @Override
  public Iterable<SerializedBlock> getSerializedBlocks(final HashRange hashRange) throws IOException {
    // Deserialize the data
    final List<SerializedBlock> blocksInRange = new ArrayList<>();
    try (final FileInputStream fileStream = new FileInputStream(filePath)) {
      for (final BlockMetadata blockMetadata : metadata.getBlockMetadataIterable()) {
        final int hashVal = blockMetadata.getHashValue();
        if (hashRange.includes(hashVal)) {
          // The hash value of this block is in the range.
          final byte[] serializedData = new byte[blockMetadata.getBlockSize()];
          final int readBytes = fileStream.read(serializedData);
          if (readBytes != serializedData.length) {
            throw new IOException("The read data size does not match with the block size.");
          }
          blocksInRange.add(new SerializedBlock(hashVal, blockMetadata.getElementsTotal(), serializedData));
        } else {
          // Have to skip this block.
          skipBytes(fileStream, blockMetadata.getBlockSize());
        }
      }
    }

    return blocksInRange;
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
    for (final BlockMetadata blockMetadata : metadata.getBlockMetadataIterable()) {
      if (hashRange.includes(blockMetadata.getHashValue())) {
        fileAreas.add(new FileArea(filePath, blockMetadata.getOffset(), blockMetadata.getBlockSize()));
      }
    }
    return fileAreas;
  }

  /**
   * Deletes the file that contains this partition data.
   * This method have to be called after all read is completed (or failed).
   *
   * @throws IOException if failed to delete.
   */
  public void deleteFile() throws IOException {
    metadata.deleteMetadata();
    Files.delete(Paths.get(filePath));
  }

  /**
   * Commits this partition to prevent further write.
   */
  @Override
  public void commit() {
    commitRemainderMetadata();
    metadata.commitPartition();
  }
}
