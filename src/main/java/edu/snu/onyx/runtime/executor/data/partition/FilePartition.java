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
import edu.snu.onyx.compiler.ir.Element;
import edu.snu.onyx.runtime.exception.PartitionWriteException;
import edu.snu.onyx.runtime.executor.data.HashRange;
import edu.snu.onyx.runtime.executor.data.metadata.BlockMetadata;
import edu.snu.onyx.runtime.executor.data.metadata.FileMetadata;
import edu.snu.onyx.runtime.executor.data.FileArea;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class represents a partition which is stored in (local or remote) file.
 */
@ThreadSafe
public final class FilePartition {
  private static final Logger LOG = LoggerFactory.getLogger(FilePartition.class.getName());
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
   *
   * @param serializedData the serialized data which will become a block.
   * @param numElement     the number of elements in the serialized data.
   * @param hashVal        the hash value of this block.
   * @throws IOException if fail to write.
   */
  public void writeBlock(final byte[] serializedData,
                         final long numElement,
                         final int hashVal) throws IOException {
    // Reserve a block write and get the metadata.
    final BlockMetadata blockMetadata = metadata.reserveBlock(hashVal, serializedData.length, numElement);

    try (
        final FileOutputStream fileOutputStream = new FileOutputStream(filePath, true);
        final FileChannel fileChannel = fileOutputStream.getChannel()
    ) {
      final StringBuilder sb = new StringBuilder();
      sb.append("WriteBlock: write data of hash value ");
      sb.append(hashVal);
      sb.append(" to ");
      sb.append(filePath);
      sb.append(" , block metadata is ");
      sb.append(blockMetadata.toString());

      // Wrap the given serialized data (but not copy it) and write.
      fileChannel.position(blockMetadata.getOffset());

      sb.append("\n");
      sb.append("The block offset is ");
      sb.append(blockMetadata.getOffset());
      sb.append(", file channel position is ");
      sb.append(fileChannel.position());
      if (blockMetadata.getOffset() != fileChannel.position()) {
        LOG.error(sb.toString());
        throw new PartitionWriteException(new Throwable("The file channel can not reach to the offset. Offset: "
            + blockMetadata.getOffset() + ", position: " + fileChannel.position()));
      }

      final ByteBuffer buf = ByteBuffer.wrap(serializedData);
      fileChannel.write(buf);

      sb.append("\n");
      sb.append("The block offset + block size is ");
      sb.append(blockMetadata.getOffset() + blockMetadata.getBlockSize());
      sb.append(", file channel position is ");
      sb.append(fileChannel.position());

      if (blockMetadata.getOffset() + blockMetadata.getBlockSize() != fileChannel.position()) {
        LOG.error(sb.toString());
        throw new PartitionWriteException(new Throwable("The file channel did not write data fully. Offset + blk size: "
            + (blockMetadata.getOffset() + blockMetadata.getBlockSize()) + ", position: " + fileChannel.position()));
      }

      if (filePath.equals("/home/ubuntu/gluster-mnt/Partition-SEdge-edge3_26")) {
        LOG.info("Write block position check: " + sb.toString());
      }
    }

    // Commit if needed.
    if (commitPerBlock) {
      metadata.commitBlocks(Collections.singleton(blockMetadata));
    } else {
      blockMetadataToCommit.add(blockMetadata);
    }
  }

  /**
   * Commits the un-committed block metadata.
   */
  public void commitRemainderMetadata() {
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
   * Retrieves the elements of this partition from the file in a specific hash range and deserializes it.
   *
   * @param hashRange the hash range
   * @return an iterable of deserialized elements.
   * @throws IOException if failed to deserialize.
   */
  public Iterable<Element> retrieveInHashRange(final HashRange hashRange) throws IOException {
    final StringBuilder sb = new StringBuilder();
    sb.append("RetrieveInHashRange: retrieve data in ");
    sb.append(hashRange.toString());
    sb.append("from ");
    sb.append(filePath);

    // Deserialize the data
    final ArrayList<Element> deserializedData = new ArrayList<>();
    try (final FileInputStream fileStream = new FileInputStream(filePath)) {
      for (final BlockMetadata blockMetadata : metadata.getBlockMetadataIterable()) {
        sb.append("\n");
        sb.append("BlockMetadata: ");
        sb.append(blockMetadata.toString());
        final int hashVal = blockMetadata.getHashValue();
        if (hashRange.includes(hashVal)) {
          // The hash value of this block is in the range.
          sb.append("\n");
          sb.append("In range block. Current file stream position is ");
          sb.append(fileStream.getChannel().position());
          deserializeBlock(blockMetadata, fileStream, deserializedData);
        } else {
          sb.append("\n");
          sb.append("Out of range block. Current file stream position is ");
          sb.append(fileStream.getChannel().position());
          // Have to skip this block.
          final long bytesToSkip = blockMetadata.getBlockSize();
          final long skippedBytes = fileStream.skip(bytesToSkip);
          sb.append("\n");
          sb.append("Bytes to skip is ");
          sb.append(bytesToSkip);
          sb.append(", skipped bytes is ");
          sb.append(skippedBytes);
          sb.append(", current file stream position is ");
          sb.append(fileStream.getChannel().position());
          if (skippedBytes != bytesToSkip) {
            throw new IOException("The file stream failed to skip to the next block.");
          }
        }
      }
    } catch (final IOException e) {
      LOG.error(sb.toString());
      throw e;
    }

    return deserializedData;
  }

  /**
   * Retrieves the list of {@link FileArea}s for the specified {@link HashRange}.
   *
   * @param hashRange     the hash range
   * @return list of the file areas
   * @throws IOException if failed to open a file channel
   */
  public List<FileArea> asFileAreas(final HashRange hashRange) throws IOException {
    final List<FileArea> fileAreas = new ArrayList<>();
    for (final BlockMetadata blockMetadata : metadata.getBlockMetadataIterable()) {
      // TODO #463: Support incremental read.
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
   * If someone "subscribing" the data in this partition, it will be finished.
   *
   * @throws IOException if failed to close.
   */
  public void commit() throws IOException {
    commitRemainderMetadata();
    metadata.commitPartition();
  }

  /**
   * Reads and deserializes a block.
   *
   * @param blockMetadata    the block metadata.
   * @param fileInputStream  the stream contains the actual data.
   * @param deserializedData the list of elements to put the deserialized data.
   * @throws IOException if fail to read and deserialize.
   */
  private void deserializeBlock(final BlockMetadata blockMetadata,
                                final FileInputStream fileInputStream,
                                final List<Element> deserializedData) throws IOException {
    final int size = blockMetadata.getBlockSize();
    final long numElements = blockMetadata.getElementsTotal();
    if (size != 0) {
      // This stream will be not closed, but it is okay as long as the file stream is closed well.
      //final BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream, size);
      for (int i = 0; i < numElements; i++) {
        //deserializedData.add(coder.decode(bufferedInputStream));
        deserializedData.add(coder.decode(fileInputStream));
      }
    }
  }
}
