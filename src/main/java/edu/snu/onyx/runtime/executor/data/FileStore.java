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
package edu.snu.onyx.runtime.executor.data;

import edu.snu.onyx.common.coder.Coder;
import edu.snu.onyx.runtime.common.RuntimeIdGenerator;
import edu.snu.onyx.runtime.executor.data.partition.FilePartition;
import org.apache.reef.tang.InjectionFuture;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Stores partitions in (local or remote) files.
 */
abstract class FileStore implements PartitionStore {

  private final String fileDirectory;
  private final InjectionFuture<PartitionManagerWorker> partitionManagerWorker;

  protected FileStore(final String fileDirectory,
                      final InjectionFuture<PartitionManagerWorker> partitionManagerWorker) {
    this.fileDirectory = fileDirectory;
    this.partitionManagerWorker = partitionManagerWorker;
  }

  /**
   * Gets the list of {@link FileArea}s for the specified partition.
   *
   * @param partitionId the partition id
   * @param hashRange   the hash range
   * @return the list of file areas
   */
  public abstract List<FileArea> getFileAreas(final String partitionId, final HashRange hashRange);

  /**
   * Makes the given stream to a block and write it to the given file partition.
   *
   * @param elementsInBlock the number of elements in this block.
   * @param outputStream    the output stream containing data.
   * @param partition       the partition to write the block.
   * @param hashVal         the hash value of the block.
   * @return the size of serialized block.
   * @throws IOException if fail to write.
   */
  private long writeBlock(final long elementsInBlock,
                          final ByteArrayOutputStream outputStream,
                          final FilePartition partition,
                          final int hashVal) throws IOException {
    final byte[] serialized = outputStream.toByteArray();
    partition.writeBlock(serialized, elementsInBlock, hashVal);

    return serialized.length;
  }

  /**
   * Gets data coder from the {@link PartitionManagerWorker}.
   *
   * @param partitionId to get the coder.
   * @return the coder.
   */
  protected Coder getCoderFromWorker(final String partitionId) {
    final PartitionManagerWorker worker = partitionManagerWorker.get();
    final String runtimeEdgeId = RuntimeIdGenerator.parsePartitionId(partitionId)[0];
    return worker.getCoder(runtimeEdgeId);
  }

  /**
   * Serializes and puts the data blocks to a file partition.
   *
   * @param coder     the coder used to serialize the data of this partition.
   * @param partition to store this data.
   * @param blocks    to be stored.
   * @return the size of the data.
   * @throws IOException if fail to write the data.
   */
  protected List<Long> putBlocks(final Coder coder,
                                 final FilePartition partition,
                                 final Iterable<Block> blocks) throws IOException {
    final List<Long> blockSizeList = new ArrayList<>();
    // Serialize the given blocks
    final ByteArrayOutputStream bytesOutputStream = new ByteArrayOutputStream();
    for (final Block block : blocks) {
      long numOfElementsInBlock = 0;
      for (final Object element : block.getData()) {
        coder.encode(element, bytesOutputStream);
        numOfElementsInBlock++;
      }

      // Write the block.
      final long blockSize = writeBlock(numOfElementsInBlock, bytesOutputStream, partition, block.getKey());
      blockSizeList.add(blockSize);
      bytesOutputStream.reset();
    }
    partition.commitRemainderMetadata();

    return blockSizeList;
  }

  /**
   * Converts a partition id to the corresponding file path.
   *
   * @param partitionId of the partition
   * @return the file path of the partition.
   */
  protected String partitionIdToFilePath(final String partitionId) {
    return fileDirectory + "/" + partitionId;
  }

  protected String getFileDirectory() {
    return fileDirectory;
  }
}
