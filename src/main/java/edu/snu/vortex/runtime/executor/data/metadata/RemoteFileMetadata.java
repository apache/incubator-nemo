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
package edu.snu.vortex.runtime.executor.data.metadata;

import edu.snu.vortex.runtime.exception.UnsupportedMethodException;

import java.io.*;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * This class represents a metadata for a remote file partition.
 * Because the data is stored in a remote file and globally accessed by multiple nodes,
 * each access (create - write - close, read, or deletion) for a file needs one instance of this metadata.
 * It supports concurrent write for a single file, but each writer has to have separate instance of this class.
 * These accesses are judiciously synchronized by the metadata server in master.
 *
 * TODO #404: Introduce metadata server model. At now, the metadata is stored in a file.
 * A single partition will have two separate files: actual data file and metadata file.
 * The actual data file will only have the serialized data.
 * The metadata file will contain the information for this data,
 * such as whether whole data is written, each block in the partition has a single hash value or not,
 * the size, offset, and number of elements in each block, etc.
 * <p>
 * /////Meta data File/////
 * /       Written        /
 * /       Hashed         /
 * ........................
 * /     hash value       /
 * /     Block size       /
 * /       offset         /
 * /    # of elements     /
 * ........................
 * /          .           /
 * /          .           /
 * /          .           /
 * ........................
 * /     hash value       /
 * /     Block size       /
 * /       offset         /
 * /    # of elements     /
 * ////////////////////////
 */
public final class RemoteFileMetadata extends FileMetadata {

  private boolean written; // The whole data for this partition is written or not yet.
  // TODO #404: Introduce metadata server model.
  private final String metaFilePath; // The path of the file that contains the metadata for this partition.

  /**
   * Creates a new file metadata to write.
   *
   * @param hashed       each block has a single hash value or not.
   * @param metaFilePath the path of the file which contains the metadata.
   */
  private RemoteFileMetadata(final boolean hashed,
                             final String metaFilePath) {
    super(hashed);
    this.written = false;
    // TODO #404: Introduce metadata server model.
    this.metaFilePath = metaFilePath;
  }

  /**
   * Opens an exist file metadata to read.
   *
   * @param hashed            each block has a single hash value or not.
   * @param metaFilePath the path of the file which contains the metadata.
   * @param blockMetadataList the list of block metadata
   */
  private RemoteFileMetadata(final boolean hashed,
                             final String metaFilePath,
                             final List<BlockMetadata> blockMetadataList) {
    super(hashed, blockMetadataList);
    this.written = true;
    // TODO #404: Introduce metadata server model.
    this.metaFilePath = metaFilePath;
  }

  /**
   * Reserves a region for storing a block and appends a metadata for the block.
   * This method is designed for concurrent write.
   * Therefore, it will communicate with the metadata server and synchronize the write.
   *
   * @param hashValue   of the block.
   * @param blockSize   of the block.
   * @param numElements of the block.
   */
  public void reserveBlock(final int hashValue,
                           final int blockSize,
                           final long numElements) {
    // TODO #404: Introduce metadata server model.
    // TODO #355: Support I-file write.
    throw new UnsupportedMethodException("reserveBlock(...) is not supported yet.");
  }

  /**
   * Marks that the whole data for this partition is written.
   * This method synchronizes all changes if needed.
   *
   * @return {@code true} if already set, or {@code false} if not.
   * @throws IOException if fail to finish the write.
   */
  @Override
  public boolean getAndSetWritten() throws IOException {
    if (written) {
      return true;
    }
    written = true;
    // TODO #404: Introduce metadata server model.
    try (
        final FileOutputStream metaFileOutputStream = new FileOutputStream(metaFilePath, true);
        final DataOutputStream metaFilePrimOutputStream = new DataOutputStream(metaFileOutputStream)
    ) {
      // Make the written boolean true to notice that the write finished.
      final FileLock fileLock = metaFileOutputStream.getChannel().tryLock();
      if (fileLock == null) {
        throw new IOException("Other thread (maybe in another node) is writing on this file.");
      }
      metaFilePrimOutputStream.writeBoolean(true); // Not written yet.
      metaFilePrimOutputStream.writeBoolean(isHashed()); // Each block has a single hash value or not.

      // Store the block metadata to the metadata file.
      for (final BlockMetadata blockMetadata : getBlockMetadataList()) {
        metaFilePrimOutputStream.writeInt(blockMetadata.getHashValue());
        metaFilePrimOutputStream.writeInt(blockMetadata.getBlockSize());
        metaFilePrimOutputStream.writeLong(blockMetadata.getOffset());
        metaFilePrimOutputStream.writeLong(blockMetadata.getNumElements());
      }

      return false;
    }
  }

  /**
   * Gets whether the whole data for this partition is written or not yet.
   *
   * @return whether the whole data for this partition is written or not yet.
   */
  @Override
  public boolean isWritten() {
    return written;
  }

  /**
   * @see FileMetadata#deleteMetadata().
   */
  @Override
  public void deleteMetadata() throws IOException {
    Files.delete(Paths.get(metaFilePath));
  }

  /**
   * Creates a file metadata for a partition in the remote storage to write.
   * The corresponding {@link FileMetadata#getAndSetWritten()}} for the returned metadata is required.
   *
   * @param filePath the path of the file which will contain the actual data.
   * @param hashed   whether each block in this partition has a single hash value or not.
   * @return the created file metadata.
   */
  public static RemoteFileMetadata create(final String filePath,
                                          final boolean hashed) {
    return new RemoteFileMetadata(hashed, filePath + "-metadata");
  }

  /**
   * Gets the corresponding file metadata for a partition in the remote storage to read.
   * It will communicates with the metadata server to get the metadata.
   *
   * @param filePath the path of the file which will contain the actual data.
   * @return the read file metadata.
   * @throws IOException if fail to read the metadata.
   */
  public static RemoteFileMetadata get(final String filePath) throws IOException {
    final List<BlockMetadata> blockMetadataList = new ArrayList<>();
    final boolean hashed;
    // TODO #410: Implement metadata caching for the RemoteFileMetadata.
    // TODO #404: Introduce metadata server model.
    final String metaFilePath = filePath + "-metadata";
    try (
        final FileInputStream metaFileInputStream = new FileInputStream(metaFilePath);
        final DataInputStream metaFilePrimInputStream = new DataInputStream(metaFileInputStream)
    ) {
      // We have to check whether the write for this file finished or not.
      final boolean written = metaFilePrimInputStream.readBoolean();
      if (!written) {
        throw new IOException("This partition is not written yet.");
      }
      hashed = metaFilePrimInputStream.readBoolean();

      // Read the block metadata.
      while (metaFileInputStream.available() > 0) {
        blockMetadataList.add(new BlockMetadata(
            metaFilePrimInputStream.readInt(), // Hash value.
            metaFilePrimInputStream.readInt(), // Block size.
            metaFilePrimInputStream.readLong(), // Offset.
            metaFilePrimInputStream.readLong() // Number of elements.
        ));
      }
    }

    return new RemoteFileMetadata(hashed, metaFilePath, blockMetadataList);
  }
}
