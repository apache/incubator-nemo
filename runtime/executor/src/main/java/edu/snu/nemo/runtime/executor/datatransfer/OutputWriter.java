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
package edu.snu.nemo.runtime.executor.datatransfer;

import edu.snu.nemo.common.KeyExtractor;
import edu.snu.nemo.common.exception.*;
import edu.snu.nemo.common.ir.edge.executionproperty.*;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.executionproperty.ExecutionProperty;
import edu.snu.nemo.runtime.common.RuntimeIdGenerator;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;
import edu.snu.nemo.runtime.executor.data.BlockManagerWorker;
import edu.snu.nemo.runtime.executor.data.Partition;
import edu.snu.nemo.runtime.executor.data.partitioner.*;

import javax.annotation.Nullable;
import java.util.*;

/**
 * Represents the output data transfer from a task.
 */
public final class OutputWriter extends DataTransfer implements AutoCloseable {
  private final String blockId;
  private final RuntimeEdge<?> runtimeEdge;
  private final String srcVertexId;
  @Nullable private final IRVertex dstIrVertex;
  private final DataStoreProperty.Value blockStoreValue;
  private final Map<PartitionerProperty.Value, Partitioner> partitionerMap;
  private final List<Long> accumulatedPartitionSizeInfo;
  private final BlockManagerWorker blockManagerWorker;

  public OutputWriter(final int hashRangeMultiplier,
                      final int srcTaskIdx,
                      final String srcRuntimeVertexId,
                      // TODO #717: Remove nullable. (If the destination is not an IR vertex, do not make OutputWriter.)
                      @Nullable final IRVertex dstIrVertex, // Null if it is not an IR vertex.
                      final RuntimeEdge<?> runtimeEdge,
                      final BlockManagerWorker blockManagerWorker) {
    super(runtimeEdge.getId());
    this.blockId = RuntimeIdGenerator.generateBlockId(getId(), srcTaskIdx);
    this.runtimeEdge = runtimeEdge;
    this.srcVertexId = srcRuntimeVertexId;
    this.dstIrVertex = dstIrVertex;
    this.blockManagerWorker = blockManagerWorker;
    this.blockStoreValue = runtimeEdge.getProperty(ExecutionProperty.Key.DataStore);
    this.partitionerMap = new HashMap<>();
    // TODO #511: Refactor metric aggregation for (general) run-rime optimization.
    this.accumulatedPartitionSizeInfo = new ArrayList<>();
    partitionerMap.put(PartitionerProperty.Value.IntactPartitioner, new IntactPartitioner());
    partitionerMap.put(PartitionerProperty.Value.HashPartitioner, new HashPartitioner());
    partitionerMap.put(PartitionerProperty.Value.DataSkewHashPartitioner,
        new DataSkewHashPartitioner(hashRangeMultiplier));
    blockManagerWorker.createBlock(blockId, blockStoreValue);
  }

  /**
   * Writes output data depending on the communication pattern of the edge.
   *
   * @param dataToWrite An iterable for the elements to be written.
   */
  public void write(final Iterable dataToWrite) {
    final Boolean isDataSizeMetricCollectionEdge = MetricCollectionProperty.Value.DataSkewRuntimePass
        .equals(runtimeEdge.getProperty(ExecutionProperty.Key.MetricCollection));

    // Group the data into blocks.
    final PartitionerProperty.Value partitionerPropertyValue =
        runtimeEdge.getProperty(ExecutionProperty.Key.Partitioner);
    final int dstParallelism = getDstParallelism();

    final Partitioner partitioner = partitionerMap.get(partitionerPropertyValue);
    if (partitioner == null) {
      throw new UnsupportedPartitionerException(
          new Throwable("Partitioner " + partitionerPropertyValue + " is not supported."));
    }

    final KeyExtractor keyExtractor = runtimeEdge.getProperty(ExecutionProperty.Key.KeyExtractor);
    final List<Partition> partitionsToWrite = partitioner.partition(dataToWrite, dstParallelism, keyExtractor);

    // Write the grouped blocks into partitions.
    // TODO #492: Modularize the data communication pattern.
    final DataCommunicationPatternProperty.Value comValue =
        runtimeEdge.getProperty(ExecutionProperty.Key.DataCommunicationPattern);

    if (DataCommunicationPatternProperty.Value.OneToOne.equals(comValue)) {
      writeOneToOne(partitionsToWrite);
    } else if (DataCommunicationPatternProperty.Value.BroadCast.equals(comValue)) {
      writeBroadcast(partitionsToWrite);
    } else if (DataCommunicationPatternProperty.Value.Shuffle.equals(comValue)) {
      // If the dynamic optimization which detects data skew is enabled, sort the data and write it.
      if (isDataSizeMetricCollectionEdge) {
        dataSkewWrite(partitionsToWrite);
      } else {
        writeShuffle(partitionsToWrite);
      }
    } else {
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
  }

  /**
   * Notifies that all writes for a block is end.
   * Subscribers waiting for the data of the target block are notified when the block is committed.
   * Also, further subscription about a committed block will not blocked but get the data in it and finished.
   */
  @Override
  public void close() {
    // Commit block.
    final UsedDataHandlingProperty.Value usedDataHandling =
        runtimeEdge.getProperty(ExecutionProperty.Key.UsedDataHandling);
    blockManagerWorker.commitBlock(blockId, blockStoreValue,
        accumulatedPartitionSizeInfo, srcVertexId, getDstParallelism(), usedDataHandling);
  }

  private void writeOneToOne(final List<Partition> partitionsToWrite) {
    // Write data.
    blockManagerWorker.putPartitions(blockId, partitionsToWrite, blockStoreValue);
  }

  private void writeBroadcast(final List<Partition> partitionsToWrite) {
    writeOneToOne(partitionsToWrite);
  }

  private void writeShuffle(final List<Partition> partitionsToWrite) {
    final int dstParallelism = getDstParallelism();
    if (partitionsToWrite.size() != dstParallelism) {
      throw new BlockWriteException(
          new Throwable("The number of given blocks are not matched with the destination parallelism."));
    }

    // Write data.
    blockManagerWorker.putPartitions(blockId, partitionsToWrite, blockStoreValue);
  }

  /**
   * Writes partitions in a single block and collects the size of each partition.
   * This function will be called only when we need to split or recombine an output data from a task after it is stored
   * (e.g., dynamic data skew handling).
   * We extend the hash range with the factor {@link edu.snu.nemo.conf.JobConf.HashRangeMultiplier} in advance
   * to prevent the extra deserialize - rehash - serialize process.
   * Each data of this block having same key hash value will be collected as a single partition.
   * This partition will be the unit of retrieval and recombination of this block.
   * Constraint: If a block is written by this method, it have to be read by {@link InputReader#readDataInRange()}.
   * TODO #378: Elaborate block construction during data skew pass
   *
   * @param partitionsToWrite a list of the partitions to be written.
   */
  private void dataSkewWrite(final List<Partition> partitionsToWrite) {

    // Write data.
    final Optional<List<Long>> partitionSizeInfo =
        blockManagerWorker.putPartitions(blockId, partitionsToWrite, blockStoreValue);
    if (partitionSizeInfo.isPresent()) {
      this.accumulatedPartitionSizeInfo.addAll(partitionSizeInfo.get());
    }
  }

  /**
   * Get the parallelism of the destination task.
   *
   * @return the parallelism of the destination task.
   */
  private int getDstParallelism() {
    return dstIrVertex == null || DataCommunicationPatternProperty.Value.OneToOne.equals(
        runtimeEdge.getProperty(ExecutionProperty.Key.DataCommunicationPattern))
        ? 1 : dstIrVertex.getProperty(ExecutionProperty.Key.Parallelism);
  }
}
