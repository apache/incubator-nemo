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
package edu.snu.vortex.runtime.executor.data;

import edu.snu.vortex.client.JobConf;
import edu.snu.vortex.common.coder.Coder;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.runtime.executor.data.partition.FilePartition;
import edu.snu.vortex.runtime.executor.data.partition.LocalPartition;
import edu.snu.vortex.runtime.executor.data.partition.Partition;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.*;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stores partitions in files.
 * It writes and reads synchronously.
 */
final class FileStore implements PartitionStore {

  private final String fileDirectory;
  private final Map<String, FilePartition> partitionIdToData;
  private final InjectionFuture<PartitionManagerWorker> partitionManagerWorker;

  @Inject
  private FileStore(@Parameter(JobConf.FileDirectory.class) final String fileDirectory,
                    final InjectionFuture<PartitionManagerWorker> partitionManagerWorker) {
    this.fileDirectory = fileDirectory;
    this.partitionIdToData = new ConcurrentHashMap<>();
    this.partitionManagerWorker = partitionManagerWorker;
    new File(fileDirectory).mkdirs();
  }

  /**
   * Retrieves a deserialized partition of data through disk.
   *
   * @param partitionId of the partition.
   * @return the partition (optionally).
   */
  @Override
  public Optional<Partition> getPartition(final String partitionId) {
    // Deserialize the target data in the corresponding file and pass it as a local data.
    final FilePartition partition = partitionIdToData.get(partitionId);
    if (partition == null) {
      return Optional.empty();
    } else {
      return Optional.of(new LocalPartition(partition.asIterable()));
    }
  }

  /**
   * Saves a partition of data as a file.
   *
   * @param partitionId of the partition.
   * @param data        of the partition.
   * @return the size of the data.
   */
  @Override
  public Optional<Long> putPartition(final String partitionId, final Iterable<Element> data) {
    final FilePartition partition = new FilePartition();
    final Partition previousPartition = partitionIdToData.putIfAbsent(partitionId, partition);
    if (previousPartition != null) {
      throw new RuntimeException("Trying to overwrite an existing partition");
    }

    // Serialize the given data
    final PartitionManagerWorker worker = partitionManagerWorker.get();
    final String runtimeEdgeId = partitionId.split("_")[1];
    final Coder coder = worker.getCoder(runtimeEdgeId);
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    long length = 0;
    // TODO 301: Divide a Task's Output Partitions into Smaller Blocks.
    for (final Element element : data) {
      coder.encode(element, outputStream);
      length++;
    }
    try {
      outputStream.close();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    final byte[] serialized = outputStream.toByteArray();

    // Synchronously write the serialized data to file
    partition.writeData(serialized, coder, fileDirectory + "/" + partitionId, length);

    return Optional.of(Long.valueOf(serialized.length));
  }

  /**
   * Removes the file that the target partition is stored.
   *
   * @param partitionId of the partition.
   * @return whether the partition exists or not.
   */
  @Override
  public boolean removePartition(final String partitionId) {
    final FilePartition serializedPartition = partitionIdToData.remove(partitionId);
    if (serializedPartition == null) {
      return false;
    } else {
      serializedPartition.deleteFile();
      return true;
    }
  }
}
