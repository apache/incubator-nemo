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
import edu.snu.vortex.runtime.executor.data.HashRange;
import edu.snu.vortex.runtime.executor.data.metadata.BlockMetadata;
import edu.snu.vortex.runtime.executor.data.metadata.FileMetadata;
import edu.snu.vortex.runtime.executor.data.FileArea;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * This class represents a {@link Partition} which is stored in (local or remote) file.
 */
public abstract class FilePartition implements Partition, AutoCloseable {

  private final Coder coder;
  private final String filePath;
  private final FileMetadata metadata;
  private FileOutputStream fileOutputStream;
  private FileChannel fileChannel;
  private boolean writable; // Whether this partition is writable or not.

  protected FilePartition(final Coder coder,
                          final String filePath,
                          final FileMetadata metadata) {
    this.coder = coder;
    this.filePath = filePath;
    this.metadata = metadata;
    this.writable = false;
  }

  /**
   * Opens file stream to write the data.
   *
   * @throws IOException if fail to open the file stream.
   */
  protected final void openFileStream() throws IOException {
    writable = true;
    this.fileOutputStream = new FileOutputStream(filePath, true);
    this.fileChannel = fileOutputStream.getChannel();
  }

  /**
   * Writes the serialized data of this partition as a block to the file where this partition resides.
   *
   * @param serializedData the serialized data which will become a block.
   * @param numElement     the number of elements in the serialized data.
   * @throws IOException if fail to write.
   */
  public final void writeBlock(final byte[] serializedData,
                               final long numElement) throws IOException {
    writeBlock(serializedData, numElement, Integer.MIN_VALUE);
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
  public final synchronized void writeBlock(final byte[] serializedData,
                                            final long numElement,
                                            final int hashVal) throws IOException {
    if (!writable) {
      throw new IOException("This partition is non-writable.");
    }
    metadata.appendBlockMetadata(hashVal, serializedData.length, numElement);

    // Wrap the given serialized data (but not copy it)
    final ByteBuffer buf = ByteBuffer.wrap(serializedData);

    // Write synchronously
    fileChannel.write(buf);
  }

  /**
   * Notice the end of write.
   *
   * @throws IOException if fail to close.
   */
  public final void finishWrite() throws IOException {
    if (!writable) {
      throw new IOException("This partition is non-writable.");
    }
    writable = false;
    if (metadata.getAndSetWritten()) {
      throw new IOException("The writing for this partition is already finished.");
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
  public final void close() throws IOException {
    if (fileChannel != null) {
      fileChannel.close();
    }
    if (fileOutputStream != null) {
      fileOutputStream.close();
    }
  }

  /**
   * Deletes the file that contains this partition data.
   * This method have to be called after all read is completed (or failed).
   *
   * @throws IOException if failed to delete.
   */
  public final void deleteFile() throws IOException {
    if (!metadata.isWritten()) {
      throw new IOException("This partition is not written yet.");
    }
    metadata.deleteMetadata();
    Files.delete(Paths.get(filePath));
  }

  /**
   * Retrieves the data of this partition from the file in a specific hash range and deserializes it.
   *
   * @param hashRange the hash range
   * @return an iterable of deserialized data.
   * @throws IOException if failed to deserialize.
   */
  public final Iterable<Element> retrieveInHashRange(final HashRange hashRange) throws IOException {
    // Check whether this partition is fully written and sorted by the hash value.
    if (!metadata.isWritten()) {
      throw new IOException("This partition is not written yet.");
    } else if (!metadata.isHashed()) {
      throw new IOException("The blocks in this partition are not hashed.");
    }

    // Deserialize the data
    final ArrayList<Element> deserializedData = new ArrayList<>();
    try (final FileInputStream fileStream = new FileInputStream(filePath)) {
      for (final BlockMetadata blockMetadata : metadata.getBlockMetadataList()) {
        final int hashVal = blockMetadata.getHashValue();
        if (hashRange.includes(hashVal)) {
          // The hash value of this block is in the range.
          deserializeBlock(blockMetadata, fileStream, deserializedData);
        } else {
          // Have to skip this block.
          final long bytesToSkip = blockMetadata.getBlockSize();
          final long skippedBytes = fileStream.skip(bytesToSkip);
          if (skippedBytes != bytesToSkip) {
            throw new IOException("The file stream failed to skip to the next block.");
          }
        }
      }
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
  public final List<FileArea> asFileAreas(final HashRange hashRange) throws IOException {
    final List<FileArea> fileAreas = new ArrayList<>();
    for (final BlockMetadata blockMetadata : metadata.getBlockMetadataList()) {
      if (hashRange.includes(blockMetadata.getHashValue())) {
        fileAreas.add(new FileArea(filePath, blockMetadata.getOffset(), blockMetadata.getBlockSize()));
      }
    }
    return fileAreas;
  }

  /**
   * @see Partition#asIterable().
   */
  @Override
  public final Iterable<Element> asIterable() throws IOException {
    // Read file synchronously
    if (!metadata.isWritten()) {
      throw new IOException("This partition is not written yet.");
    }

    // Deserialize the data
    final ArrayList<Element> deserializedData = new ArrayList<>();
    try (final FileInputStream fileStream = new FileInputStream(filePath)) {
      metadata.getBlockMetadataList().forEach(blockInfo -> {
        deserializeBlock(blockInfo, fileStream, deserializedData);
      });
    }

    return deserializedData;
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
                                final List<Element> deserializedData) {
    final int size = blockMetadata.getBlockSize();
    final long numElements = blockMetadata.getNumElements();
    if (size != 0) {
      // This stream will be not closed, but it is okay as long as the file stream is closed well.
      final BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream, size);
      for (int i = 0; i < numElements; i++) {
        deserializedData.add(coder.decode(bufferedInputStream));
      }
    }
  }

  protected final FileChannel getFileChannel() {
    return fileChannel;
  }
}
