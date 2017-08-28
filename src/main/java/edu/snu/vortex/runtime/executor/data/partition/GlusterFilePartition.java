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
import edu.snu.vortex.runtime.executor.data.metadata.RemoteFileMetadata;

import java.io.*;
import java.nio.channels.FileLock;
import java.util.Optional;

/**
 * This class implements the {@link Partition} which is stored in a GlusterFS volume.
 * Because the data is stored in a remote file and globally accessed by multiple nodes,
 * each access (create - write - close, read, or deletion) for a file needs one instance of this partition.
 * It supports concurrent write for a single file, but each writer has to have separate instance of this class.
 * These accesses are judiciously synchronized by the metadata server in master.
 */
public final class GlusterFilePartition extends FilePartition {

  /**
   * Constructs a gluster file partition.
   *
   * @param coder        the coder used to serialize and deserialize the data of this partition.
   * @param dataFilePath the path of the file which will contain the data of this partition.
   * @param metadata     the metadata for this partition.
   */
  private GlusterFilePartition(final Coder coder,
                               final String dataFilePath,
                               final RemoteFileMetadata metadata) {
    super(coder, dataFilePath, metadata);
  }

  /**
   * Prepare the partition to write. The corresponding {@link FilePartition#finishWrite()} is required.
   *
   * @throws IOException if fail to open this partition for writing.
   */
  private void initializeWrite() throws IOException {
    openFileStream();

    if (!((RemoteFileMetadata) getMetadata()).needToSyncPerWrite()) {
      // Prevent concurrent write by using the file lock of this file.
      // If once this lock is acquired, it have to be released to prevent the locked leftover in the remote storage.
      // Because this lock will be released when the file channel is closed, we need to close the file channel well.
      final FileLock fileLock = getFileChannel().tryLock();
      if (fileLock == null) {
        throw new IOException("Other thread (maybe in another node) is writing on this file.");
      }
    }
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
  @Override
  public void writeBlock(final byte[] serializedData,
                         final long numElement,
                         final int hashVal) throws IOException {
    if (!isWritable()) {
      throw new IOException("This partition is non-writable.");
    }

    // Reserve the block write and move to the reserved position.
    final long positionToWrite =
        getMetadata().appendBlockMetadata(hashVal, serializedData.length, numElement);
    getFileChannel().position(positionToWrite);

    if (((RemoteFileMetadata) getMetadata()).needToSyncPerWrite()) {
      try (final FileLock fileLock = getFileChannel().tryLock(positionToWrite, serializedData.length, false)) {
        if (fileLock == null) {
          throw new IOException("Other thread (maybe in another node) is writing on this file region.");
        }

        writeBytes(serializedData);
      }
    } else {
      writeBytes(serializedData);
    }
  }

  /**
   * Opens the corresponding file for this partition in the storage to write.
   * It creates a file if does not exist.
   * The corresponding {@link FilePartition#finishWrite()} for the returned partition is required.
   *
   * @param coder         the coder used to serialize and deserialize the data of this partition.
   * @param filePath      the path of the file which will contain the data of this partition.
   * @param metadata      the metadata for this partition.
   * @return the corresponding partition.
   * @throws IOException if fail to create the file exist already.
   */
  public static GlusterFilePartition openToWrite(final Coder coder,
                                                 final String filePath,
                                                 final RemoteFileMetadata metadata) throws IOException {
    final GlusterFilePartition partition = new GlusterFilePartition(coder, filePath, metadata);
    partition.initializeWrite();
    return partition;
  }

  /**
   * Opens the corresponding file for this partition in the storage to read.
   *
   * @param coder    the coder used to serialize and deserialize the data of this partition.
   * @param filePath the path of the file which will contain the data of this partition.
   * @param metadata the metadata for this partition.
   * @return the partition if success to open the file and partition, or an empty optional if the file does not exist.
   */
  public static Optional<GlusterFilePartition> openToRead(final Coder coder,
                                                          final String filePath,
                                                          final RemoteFileMetadata metadata) {
    if (new File(filePath).isFile()) {
      return Optional.of(new GlusterFilePartition(coder, filePath, metadata));
    } else {
      return Optional.empty();
    }
  }
}
