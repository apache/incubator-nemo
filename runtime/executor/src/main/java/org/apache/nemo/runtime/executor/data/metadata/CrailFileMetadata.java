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
package org.apache.nemo.runtime.executor.data.metadata;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.crail.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * This class represents a metadata for a file block in CrailFileSystem.
 * Because the data is stored in a CrailFileSystem and globally accessed by multiple nodes,
 * each read, or deletion for a block needs one instance of this metadata.
 * The metadata is stored in and read from a CrailFile (after a CrailFile block is committed).
 * @param <K> the key type of its partitions.
 */
@ThreadSafe
public final class CrailFileMetadata<K extends Serializable> extends FileMetadata<K> {
  private static final Logger LOG = LoggerFactory.getLogger(CrailFileMetadata.class.getName());
  private final String metaFilePath;
  private static CrailStore fs;

  /**
   * Constructor for creating a non-committed new CrailFile metadata.
   *
   * @param metaFilePath the metadata file path.
   * @param fs           the CrailStore instance.
   */
  private CrailFileMetadata(final String metaFilePath, final CrailStore fs) {
    super();
    this.metaFilePath = metaFilePath;
    this.fs = fs;
  }

  /**
   * Constructor for opening a existing CrailFile metadata.
   *
   * @param metaFilePath          the metadata file path.
   * @param partitionMetadataList the partition metadata list.
   * @param fs                    the CrailStore instance.
   */
  private CrailFileMetadata(final String metaFilePath,
                            final List<PartitionMetadata<K>> partitionMetadataList, final CrailStore fs) {
    super(partitionMetadataList);
    this.metaFilePath = metaFilePath;
    this.fs = fs;
  }

  /**
   * @see FileMetadata#deleteMetadata()
   */
  @Override
  public void deleteMetadata() throws IOException {
    try {
      fs.delete(metaFilePath, true).get().syncDir();
    } catch (Exception e) {
      LOG.info("HY: metadata deletion failed");
      e.printStackTrace();
    }
  }

  /**
   * Write the collected {@link PartitionMetadata}s to the metadata file.
   * Notifies that all writes are finished for the block corresponding to this metadata.
   */
  @Override
  public synchronized void commitBlock() throws IOException {
    LOG.info("HY: metadata commit for block {}", metaFilePath);
    final Iterable<PartitionMetadata<K>> partitionMetadataItr = getPartitionMetadataList();
    try {
      CrailBufferedOutputStream metaFileOutputstream =
        fs.create(metaFilePath, CrailNodeType.DATAFILE, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT, true)
          .get().asFile().getBufferedOutputStream(0);
      for (PartitionMetadata<K> partitionMetadata : partitionMetadataItr) {
        final byte[] key = SerializationUtils.serialize(partitionMetadata.getKey());
        metaFileOutputstream.writeInt(key.length);
        metaFileOutputstream.write(key);
        metaFileOutputstream.writeInt(partitionMetadata.getPartitionSize());
        metaFileOutputstream.writeLong(partitionMetadata.getOffset());
      }
      metaFileOutputstream.close();
    } catch (Exception e) {
      LOG.info("HY: CrailBufferedOutputStream exception occurred");
      e.printStackTrace();
    }
    setCommitted(true);
  }

  /**
   * Creates a new block metadata.
   *
   * @param metaFilePath the path of the file to write metadata.
   * @param fs           the CrailStore instance.
   * @param <T>          the key type of the block's partitions.
   * @return the created block metadata.
   */
  public static <T extends Serializable> CrailFileMetadata<T> create(final String metaFilePath, final CrailStore fs) {
    return new CrailFileMetadata<>(metaFilePath, fs);
  }

  /**
   * Opens a existing block metadata in file.
   *
   * @param metaFilePath the path of the file to write metadata.
   * @param fs           the CrailStore instance
   * @param <T>          the key type of the block's partitions.
   * @return the created block metadata.
   * @throws IOException if fail to open.
   */
  public static <T extends Serializable> CrailFileMetadata<T> open(final String metaFilePath,
                                                                   final CrailStore fs) throws IOException {
    LOG.info("HY: metafilePath {}", metaFilePath);
    final List<PartitionMetadata<T>> partitionMetadataList = new ArrayList<>();
    try {
      CrailBufferedInputStream dataInputStream = fs.lookup(metaFilePath).get().asFile().getBufferedInputStream(0);
      while (dataInputStream.available() > 0) {
        final int keyLength = dataInputStream.readInt();
        final byte[] desKey = new byte[keyLength];
        if (keyLength != dataInputStream.read(desKey)) {
          throw new IOException("Invalid key length!");
        }

        final PartitionMetadata<T> partitionMetadata = new PartitionMetadata<>(
          SerializationUtils.deserialize(desKey),
          dataInputStream.readInt(),
          dataInputStream.readLong()
        );
        partitionMetadataList.add(partitionMetadata);
      }
    } catch (Exception e) {
      throw new IOException("HY: File " + metaFilePath + " does not exist!");
    }
    return new CrailFileMetadata<>(metaFilePath, partitionMetadataList, fs);
  }
}
