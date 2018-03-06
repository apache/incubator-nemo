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
package edu.snu.nemo.runtime.executor.data.metadata;

import org.apache.commons.lang3.SerializationUtils;

import javax.annotation.concurrent.ThreadSafe;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * This class represents a metadata for a remote file block.
 * Because the data is stored in a remote file and globally accessed by multiple nodes,
 * each read, or deletion for a block needs one instance of this metadata.
 * The metadata is store in and read from a file (after a remote file block is committed).
 * @param <K> the key type of its partitions.
 */
@ThreadSafe
public final class RemoteFileMetadata<K extends Serializable> extends FileMetadata<K> {

  private final String metaFilePath;

  /**
   * Constructor for creating a non-committed new file metadata.
   *
   * @param metaFilePath the metadata file path.
   */
  private RemoteFileMetadata(final String metaFilePath) {
    super();
    this.metaFilePath = metaFilePath;
  }

  /**
   * Constructor for opening a existing file metadata.
   *
   * @param metaFilePath          the metadata file path.
   * @param partitionMetadataList the partition metadata list.
   */
  private RemoteFileMetadata(final String metaFilePath,
                             final List<PartitionMetadata<K>> partitionMetadataList) {
    super(partitionMetadataList);
    this.metaFilePath = metaFilePath;
  }

  /**
   * @see FileMetadata#deleteMetadata()
   */
  @Override
  public void deleteMetadata() throws IOException {
    Files.delete(Paths.get(metaFilePath));
  }

  /**
   * Write the collected {@link PartitionMetadata}s to the metadata file.
   * Notifies that all writes are finished for the block corresponding to this metadata.
   */
  @Override
  public synchronized void commitBlock() throws IOException {
    final Iterable<PartitionMetadata<K>> partitionMetadataItr = getPartitionMetadataList();
    try (
        final FileOutputStream metafileOutputStream = new FileOutputStream(metaFilePath, false);
        final DataOutputStream dataOutputStream = new DataOutputStream(metafileOutputStream)
    ) {
      for (PartitionMetadata<K> partitionMetadata : partitionMetadataItr) {
        final byte[] key = SerializationUtils.serialize(partitionMetadata.getKey());
        dataOutputStream.writeInt(key.length);
        dataOutputStream.write(key);
        dataOutputStream.writeInt(partitionMetadata.getPartitionSize());
        dataOutputStream.writeLong(partitionMetadata.getOffset());
        dataOutputStream.writeLong(partitionMetadata.getElementsTotal());
      }
    }
    setCommitted(true);
  }

  /**
   * Creates a new block metadata.
   *
   * @param metaFilePath the path of the file to write metadata.
   * @param <T>          the key type of the block's partitions.
   * @return the created block metadata.
   */
  public static <T extends Serializable> RemoteFileMetadata<T> create(final String metaFilePath) {
    return new RemoteFileMetadata<>(metaFilePath);
  }

  /**
   * Opens a existing block metadata in file.
   *
   * @param metaFilePath the path of the file to write metadata.
   * @param <T>          the key type of the block's partitions.
   * @return the created block metadata.
   * @throws IOException if fail to open.
   */
  public static <T extends Serializable> RemoteFileMetadata<T> open(final String metaFilePath) throws IOException {
    if (!new File(metaFilePath).isFile()) {
      throw new IOException("File " + metaFilePath + " does not exist!");
    }
    final List<PartitionMetadata<T>> partitionMetadataList = new ArrayList<>();
    try (
        final FileInputStream metafileInputStream = new FileInputStream(metaFilePath);
        final DataInputStream dataInputStream = new DataInputStream(metafileInputStream)
    ) {
      while (dataInputStream.available() > 0) {
        final int keyLength = dataInputStream.readInt();
        final byte[] desKey = new byte[keyLength];
        if (keyLength != dataInputStream.read(desKey)) {
          throw new IOException("Invalid key length!");
        }

        final PartitionMetadata<T> partitionMetadata = new PartitionMetadata<>(
            SerializationUtils.deserialize(desKey),
            dataInputStream.readInt(),
            dataInputStream.readLong(),
            dataInputStream.readLong()
        );
        partitionMetadataList.add(partitionMetadata);
      }
    }
    return new RemoteFileMetadata<>(metaFilePath, partitionMetadataList);
  }
}
