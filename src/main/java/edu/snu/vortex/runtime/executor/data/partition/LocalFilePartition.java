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

/**
 * This class represents a {@link Partition} which is stored in file.
 * It does not contain any actual data.
 */
public final class LocalFilePartition implements Partition {

  private final AtomicBoolean opened;
  private final AtomicBoolean written;
  private final Coder coder;
  private final String filePath;
  private final List<BlockInfo> blockInfoList;
  private FileOutputStream fileOutputStream;
  private FileChannel fileChannel;

  /**
   * Constructs a file partition.
   *
   * @param coder    the coder used to serialize and deserialize the data of this partition.
   * @param filePath the path of the file which will contain the data of this partition.
   */
  public LocalFilePartition(final Coder coder,
                            final String filePath) {
    this.coder = coder;
    this.filePath = filePath;
    opened = new AtomicBoolean(false);
    written = new AtomicBoolean(false);
    blockInfoList = new ArrayList<>();
  }

  /**
   * Opens partition for writing. The corresponding {@link LocalFilePartition#finishWrite()} is required.
   * @throws RuntimeException if failed to open
   */
  public void openPartitionForWrite() throws RuntimeException {
    if (opened.getAndSet(true)) {
      throw new RuntimeException("Trying to re-open a partition for write");
    }
    try {
      fileOutputStream = new FileOutputStream(filePath, true);
      fileChannel = fileOutputStream.getChannel();
    } catch (final FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Writes the serialized data of this partition as a block to the file where this partition resides.
   *
   * @param serializedData the serialized data of this partition.
   * @param numElement     the number of elements in the serialized data.
   * @throws RuntimeException if failed to write.
   */
  public void writeBlock(final byte[] serializedData,
                         final long numElement) throws RuntimeException {
    if (!opened.get()) {
      throw new RuntimeException("Trying to write a block in a partition that has not been opened for write.");
    }
    blockInfoList.add(new BlockInfo(serializedData.length, numElement));
    // Wrap the given serialized data (but not copy it)
    final ByteBuffer buf = ByteBuffer.wrap(serializedData);

    // Write synchronously
    try {
      fileChannel.write(buf);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Notice the end of write.
   * @throws RuntimeException if failed to close.
   */
  public void finishWrite() throws RuntimeException {
    if (!opened.get()) {
      throw new RuntimeException("Trying to finish writing a partition that has not been opened for write.");
    }
    if (written.getAndSet(true)) {
      throw new RuntimeException("Trying to finish writing that has been already finished.");
    }
    try {
      fileChannel.close();
      fileOutputStream.close();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Deletes the file that contains this partition data.
   *
   * @throws RuntimeException if failed to delete.
   */
  public void deleteFile() throws RuntimeException {
    if (!written.get()) {
      throw new RuntimeException("This partition is not written yet.");
    }
    try {
      Files.delete(Paths.get(filePath));
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Read the data of this partition from the file and deserialize it.
   *
   * @return an iterable of deserialized data.
   * @throws RuntimeException if failed to deserialize.
   */
  @Override
  public Iterable<Element> asIterable() throws RuntimeException {
    // Read file synchronously
    if (!written.get()) {
      throw new RuntimeException("This partition is not written yet.");
    }

    // Deserialize the data
    final ArrayList<Element> deserializedData = new ArrayList<>();
    try (final FileInputStream fileStream = new FileInputStream(filePath)) {
      blockInfoList.forEach(blockInfo -> {
        // Deserialize a block
        final int size = blockInfo.getBlockSize();
        final long numElements = blockInfo.getNumElements();
        if (size != 0) {
          // This stream will be not closed, but it is okay as long as the file stream is closed well.
          final BufferedInputStream bufferedInputStream = new BufferedInputStream(fileStream, size);
          for (int i = 0; i < numElements; i++) {
            deserializedData.add(coder.decode(bufferedInputStream));
          }
        }
      });
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }

    return deserializedData;
  }

  /**
   * This class represents the block information.
   */
  private final class BlockInfo {
    private final int blockSize;
    private final long numElements;

    private BlockInfo(final int blockSize,
                      final long numElements) {
      this.blockSize = blockSize;
      this.numElements = numElements;
    }

    private int getBlockSize() {
      return blockSize;
    }

    private long getNumElements() {
      return numElements;
    }
  }
}
