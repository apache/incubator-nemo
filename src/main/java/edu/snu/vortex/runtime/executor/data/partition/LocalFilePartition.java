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
package edu.snu.vortex.runtime.executor.data.partition;

import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.ir.Element;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * This class implements the {@link FilePartition} which is stored in a local file.
 * This partition have to be treated as an actual file
 * (i.e., construction and removal of this partition means the creation and deletion of the file),
 * even though the actual data is stored only in the local disk.
 * Also, to prevent the memory leak, this partition have to be closed when any exception is occurred during write.
 */
public final class LocalFilePartition implements FilePartition {

  private final AtomicBoolean opened;
  private final AtomicBoolean written;
  private final Coder coder;
  private final String filePath;
  private final List<BlockInfo> blockInfoList;
  private FileOutputStream fileOutputStream;
  private FileChannel fileChannel;
  private final boolean sorted; // Whether this partition is sorted by the hash value or not.
  private long writtenBytes; // The written bytes in this file.

  /**
   * Constructs a local file partition.
   *
   * @param coder    the coder used to serialize and deserialize the data of this partition.
   * @param filePath the path of the file which will contain the data of this partition.
   * @param sorted   whether the blocks in this partition are sorted by the hash value or not.
   */
  public LocalFilePartition(final Coder coder,
                            final String filePath,
                            final boolean sorted) {
    this.coder = coder;
    this.filePath = filePath;
    this.sorted = sorted;
    opened = new AtomicBoolean(false);
    written = new AtomicBoolean(false);
    blockInfoList = new ArrayList<>();
    writtenBytes = 0;
  }

  /**
   * Opens partition for writing. The corresponding {@link LocalFilePartition#finishWrite()} is required.
   *
   * @throws IOException if fail to open this partition.
   */
  public void openPartitionForWrite() throws IOException {
    if (opened.getAndSet(true)) {
      throw new IOException("Trying to re-open a partition for write");
    }
    fileOutputStream = new FileOutputStream(filePath, true);
    fileChannel = fileOutputStream.getChannel();
  }

  /**
   * @see FilePartition#writeBlock(byte[], long).
   */
  @Override
  public void writeBlock(final byte[] serializedData,
                         final long numElement) throws IOException {
    if (!opened.get()) {
      throw new IOException("Trying to write a block in a partition that has not been opened for write.");
    }
    blockInfoList.add(new BlockInfo(serializedData.length, numElement, writtenBytes));

    // Wrap the given serialized data (but not copy it)
    final ByteBuffer buf = ByteBuffer.wrap(serializedData);

    // Write synchronously
    fileChannel.write(buf);
    writtenBytes += serializedData.length;
  }

  /**
   * Notice the end of write.
   *
   * @throws IOException if fail to close.
   */
  public void finishWrite() throws IOException {
    if (!opened.get()) {
      throw new IOException("Trying to finish writing a partition that has not been opened for write.");
    }
    if (written.getAndSet(true)) {
      throw new IOException("Trying to finish writing that has been already finished.");
    }
    this.close();
  }

  /**
   * Closes the file channel and stream if opened.
   * It does not mean that this partition becomes invalid, but just cannot be written anymore.
   *
   * @throws IOException if fail to close.
   */
  @Override
  public void close() throws IOException {
    if (fileChannel != null) {
      fileChannel.close();
    }
    if (fileOutputStream != null) {
      fileOutputStream.close();
    }
  }

  /**
   * @see FilePartition#deleteFile().
   */
  @Override
  public void deleteFile() throws IOException {
    if (!written.get()) {
      throw new IOException("This partition is not written yet.");
    }
    Files.delete(Paths.get(filePath));
  }

  /**
   * @see FilePartition#retrieveInHashRange(int, int);
   */
  @Override
  public Iterable<Element> retrieveInHashRange(final int hashRangeStartVal,
                                               final int hashRangeEndVal) throws IOException {
    // Check whether this partition is fully written and sorted by the hash value.
    if (!written.get()) {
      throw new IOException("This partition is not written yet.");
    } else if (!sorted) {
      throw new IOException("The blocks in this partition are not sorted.");
    }

    // Deserialize the data
    final ArrayList<Element> deserializedData = new ArrayList<>();
    try (final FileInputStream fileStream = new FileInputStream(filePath)) {
      // Skip to the offset of the first block.
      final long startOffset = blockInfoList.get(hashRangeStartVal).getOffset();
      final long skippedBytes = fileStream.skip(startOffset);
      if (skippedBytes != startOffset) {
        throw new IOException("The file stream failed to skip to the offset.");
      }

      IntStream.range(hashRangeStartVal, hashRangeEndVal).forEach(hashVal -> {
        final BlockInfo blockInfo = blockInfoList.get(hashVal);
        deserializeBlock(blockInfo, fileStream, deserializedData);
      });
    }

    return deserializedData;
  }

  /**
   * Read the data of this partition from the file and deserialize it.
   *
   * @return an iterable of deserialized data.
   * @throws IOException if fail to deserialize.
   */
  @Override
  public Iterable<Element> asIterable() throws IOException {
    // Read file synchronously
    if (!written.get()) {
      throw new IOException("This partition is not written yet.");
    }

    // Deserialize the data
    final ArrayList<Element> deserializedData = new ArrayList<>();
    try (final FileInputStream fileStream = new FileInputStream(filePath)) {
      blockInfoList.forEach(blockInfo -> {
        deserializeBlock(blockInfo, fileStream, deserializedData);
      });
    }

    return deserializedData;
  }

  /**
   * Reads and deserializes a block.
   *
   * @param blockInfo        the block information.
   * @param fileInputStream  the stream contains the actual data.
   * @param deserializedData the list of elements to put the deserialized data.
   * @throws IOException if fail to read and deserialize.
   */
  private void deserializeBlock(final BlockInfo blockInfo,
                                final FileInputStream fileInputStream,
                                final List<Element> deserializedData) {
    final int size = blockInfo.getBlockSize();
    final long numElements = blockInfo.getNumElements();
    if (size != 0) {
      // This stream will be not closed, but it is okay as long as the file stream is closed well.
      final BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream, size);
      for (int i = 0; i < numElements; i++) {
        deserializedData.add(coder.decode(bufferedInputStream));
      }
    }
  }

  /**
   * This class represents the block information.
   */
  private final class BlockInfo {
    private final int blockSize;
    private final long numElements;
    private final long offset; // The byte offset of this block in this file.

    private BlockInfo(final int blockSize,
                      final long numElements,
                      final long offset) {
      this.blockSize = blockSize;
      this.numElements = numElements;
      this.offset = offset;
    }

    private int getBlockSize() {
      return blockSize;
    }

    private long getNumElements() {
      return numElements;
    }

    private long getOffset() {
      return offset;
    }
  }
}
