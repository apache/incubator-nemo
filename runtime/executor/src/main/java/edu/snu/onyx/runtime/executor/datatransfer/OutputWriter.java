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
package edu.snu.onyx.runtime.executor.datatransfer;

import edu.snu.onyx.common.KeyExtractor;
import edu.snu.onyx.common.exception.*;
import edu.snu.onyx.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.onyx.common.ir.edge.executionproperty.DataStoreProperty;
import edu.snu.onyx.common.ir.edge.executionproperty.MetricCollectionProperty;
import edu.snu.onyx.common.ir.edge.executionproperty.PartitionerProperty;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.runtime.common.RuntimeIdGenerator;
import edu.snu.onyx.runtime.common.plan.RuntimeEdge;
import edu.snu.onyx.runtime.executor.data.partitioner.*;
import edu.snu.onyx.runtime.common.data.Block;
import edu.snu.onyx.runtime.executor.data.PartitionManagerWorker;

import javax.annotation.Nullable;
import java.util.*;

/**
 * Represents the output data transfer from a task.
 */
public final class OutputWriter extends DataTransfer implements AutoCloseable {
  private final String partitionId;
  private final RuntimeEdge<?> runtimeEdge;
  private final String srcVertexId;
  private final IRVertex dstVertex;
  private final DataStoreProperty.Value channelDataPlacement;
  private final Map<PartitionerProperty.Value, Partitioner> partitionerMap;
  private final List<Long> accumulatedBlockSizeInfo;

  /**
   * The Block Manager Worker.
   */
  private final PartitionManagerWorker partitionManagerWorker;

  public OutputWriter(final int hashRangeMultiplier,
                      final int srcTaskIdx,
                      final String srcRuntimeVertexId,
                      @Nullable final IRVertex dstRuntimeVertex, // Null if it is not a runtime vertex.
                      final RuntimeEdge<?> runtimeEdge,
                      final PartitionManagerWorker partitionManagerWorker) {
    super(runtimeEdge.getId());
    this.partitionId = RuntimeIdGenerator.generatePartitionId(getId(), srcTaskIdx);
    this.runtimeEdge = runtimeEdge;
    this.srcVertexId = srcRuntimeVertexId;
    this.dstVertex = dstRuntimeVertex;
    this.partitionManagerWorker = partitionManagerWorker;
    this.channelDataPlacement = runtimeEdge.getProperty(ExecutionProperty.Key.DataStore);
    this.partitionerMap = new HashMap<>();
    // TODO #511: Refactor metric aggregation for (general) run-rime optimization.
    this.accumulatedBlockSizeInfo = new ArrayList<>();
    // TODO #535: Enable user to create new implementation of each execution property.
    partitionerMap.put(PartitionerProperty.Value.IntactPartitioner, new IntactPartitioner());
    partitionerMap.put(PartitionerProperty.Value.HashPartitioner, new HashPartitioner());
    partitionerMap.put(PartitionerProperty.Value.DataSkewHashPartitioner,
        new DataSkewHashPartitioner(hashRangeMultiplier));
    partitionManagerWorker.createPartition(partitionId, channelDataPlacement);
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
      // TODO #535: Enable user to create new implementation of each execution property.
      throw new UnsupportedPartitionerException(
          new Throwable("Partitioner " + partitionerPropertyValue + " is not supported."));
    }

    final KeyExtractor keyExtractor = runtimeEdge.getProperty(ExecutionProperty.Key.KeyExtractor);
    final List<Block> blocksToWrite = partitioner.partition(dataToWrite, dstParallelism, keyExtractor);

    // Write the grouped blocks into partitions.
    // TODO #492: Modularize the data communication pattern.
    DataCommunicationPatternProperty.Value comValue =
        runtimeEdge.getProperty(ExecutionProperty.Key.DataCommunicationPattern);

    if (DataCommunicationPatternProperty.Value.OneToOne.equals(comValue)) {
      writeOneToOne(blocksToWrite);
    } else if (DataCommunicationPatternProperty.Value.BroadCast.equals(comValue)) {
      writeBroadcast(blocksToWrite);
    } else if (DataCommunicationPatternProperty.Value.ScatterGather.equals(comValue)) {
      // If the dynamic optimization which detects data skew is enabled, sort the data and write it.
      if (isDataSizeMetricCollectionEdge) {
        dataSkewWrite(blocksToWrite);
      } else {
        writeScatterGather(blocksToWrite);
      }
    } else {
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
  }

  /**
   * Notifies that all writes for a partition is end.
   * Subscribers waiting for the data of the target partition are notified when the partition is committed.
   * Also, further subscription about a committed partition will not blocked but get the data in it and finished.
   */
  @Override
  public void close() {
    // Commit partition.
    partitionManagerWorker.commitPartition(partitionId, channelDataPlacement, accumulatedBlockSizeInfo, srcVertexId);
  }

  private void writeOneToOne(final List<Block> blocksToWrite) {
    // Write data.
    partitionManagerWorker.putBlocks(
        partitionId, blocksToWrite, channelDataPlacement, false);
  }

  private void writeBroadcast(final List<Block> blocksToWrite) {
    writeOneToOne(blocksToWrite);
  }

  private void writeScatterGather(final List<Block> blocksToWrite) {
    final int dstParallelism = getDstParallelism();
    if (blocksToWrite.size() != dstParallelism) {
      throw new PartitionWriteException(
          new Throwable("The number of given blocks are not matched with the destination parallelism."));
    }

    // Write data.
    partitionManagerWorker.putBlocks(
        partitionId, blocksToWrite, channelDataPlacement, false);
  }

  /**
   * Writes blocks in a single partition and collects the size of each block.
   * This function will be called only when we need to split or recombine an output data from a task after it is stored
   * (e.g., dynamic data skew handling).
   * We extend the hash range with the factor {@link edu.snu.onyx.conf.JobConf.HashRangeMultiplier} in advance
   * to prevent the extra deserialize - rehash - serialize process.
   * Each data of this partition having same key hash value will be collected as a single block.
   * This block will be the unit of retrieval and recombination of this partition.
   * Constraint: If a partition is written by this method, it have to be read by {@link InputReader#readDataInRange()}.
   * TODO #378: Elaborate block construction during data skew pass
   * TODO #428: DynOpt-clean up the metric collection flow
   *
   * @param blocksToWrite a list of the blocks to be written.
   */
  private void dataSkewWrite(final List<Block> blocksToWrite) {

    // Write data.
    final Optional<List<Long>> blockSizeInfo =
        partitionManagerWorker.putBlocks(partitionId, blocksToWrite, channelDataPlacement, false);
    if (blockSizeInfo.isPresent()) {
      this.accumulatedBlockSizeInfo.addAll(blockSizeInfo.get());
    }
  }

  /**
   * Get the parallelism of the destination task.
   *
   * @return the parallelism of the destination task.
   */
  private int getDstParallelism() {
    return dstVertex == null ? 1 : dstVertex.getProperty(ExecutionProperty.Key.Parallelism);
  }
}
