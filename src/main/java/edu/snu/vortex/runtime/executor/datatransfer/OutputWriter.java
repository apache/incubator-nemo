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
package edu.snu.vortex.runtime.executor.datatransfer;

import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.optimizer.pass.runtime.DataSkewRuntimePass;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.exception.*;
import edu.snu.vortex.runtime.executor.data.Block;
import edu.snu.vortex.runtime.executor.data.PartitionManagerWorker;
import edu.snu.vortex.runtime.executor.data.PartitionStore;
import edu.snu.vortex.runtime.executor.datatransfer.communication.Broadcast;
import edu.snu.vortex.runtime.executor.datatransfer.communication.OneToOne;
import edu.snu.vortex.runtime.executor.datatransfer.communication.ScatterGather;
import edu.snu.vortex.runtime.executor.datatransfer.partitioning.*;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Represents the output data transfer from a task.
 */
public final class OutputWriter extends DataTransfer {
  private final int srcTaskIdx;
  private final RuntimeEdge<?> runtimeEdge;
  private final String srcVertexId;
  private final IRVertex dstVertex;
  private final Class<? extends PartitionStore> channelDataPlacement;
  private final Map<Class<? extends Partitioner>, Partitioner> partitionerMap;

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
    this.runtimeEdge = runtimeEdge;
    this.srcVertexId = srcRuntimeVertexId;
    this.dstVertex = dstRuntimeVertex;
    this.partitionManagerWorker = partitionManagerWorker;
    this.srcTaskIdx = srcTaskIdx;
    this.channelDataPlacement = runtimeEdge.getProperty(ExecutionProperty.Key.DataStore);
    this.partitionerMap = new HashMap<>();
    // TODO #535: Enable user to create new implementation of each execution property.
    partitionerMap.put(IntactPartitioner.class, new IntactPartitioner());
    partitionerMap.put(HashPartitioner.class, new HashPartitioner());
    partitionerMap.put(DataSkewHashPartitioner.class, new DataSkewHashPartitioner(hashRangeMultiplier));
  }

  /**
   * Writes output data depending on the communication pattern of the edge.
   *
   * @param dataToWrite An iterable for the elements to be written.
   */
  public void write(final Iterable<Element> dataToWrite) {
    final Boolean isDataSizeMetricCollectionEdge = DataSkewRuntimePass.class
        .equals(runtimeEdge.getProperty(ExecutionProperty.Key.MetricCollection));

    try {
      // Group the data into blocks.
      final Class<? extends Partitioner> partitionerClass =
          runtimeEdge.<Class>getProperty(ExecutionProperty.Key.Partitioner);
      final int dstParallelism;
      if (dstVertex == null) {
        // Task group internal edge.
        dstParallelism = 1;
      } else {
        // Inter stage edge.
        dstParallelism = dstVertex.getProperty(ExecutionProperty.Key.Parallelism);
      }

      final Partitioner partitioner = partitionerMap.get(partitionerClass);
      if (partitioner == null) {
        // TODO #535: Enable user to create new implementation of each execution property.
        throw new UnsupportedPartitionerException(
            new Throwable("Partitioner " + partitionerClass + " is not supported."));
      }
      final List<Block> blocksToWrite = partitioner.partition(dataToWrite, dstParallelism);

      // Write the grouped blocks into partitions.
      // TODO #492: Modularize the data communication pattern.
      switch ((runtimeEdge.<Class>getProperty(ExecutionProperty.Key.DataCommunicationPattern)).getSimpleName()) {
        case OneToOne.SIMPLE_NAME:
          writeOneToOne(blocksToWrite);
          break;
        case Broadcast.SIMPLE_NAME:
          writeBroadcast(blocksToWrite);
          break;
        case ScatterGather.SIMPLE_NAME:
          // If the dynamic optimization which detects data skew is enabled, sort the data and write it.
          if (isDataSizeMetricCollectionEdge) {
            dataSkewWrite(blocksToWrite);
          } else {
            writeScatterGather(blocksToWrite);
          }
          break;
        default:
          throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
      }
    } catch (final InterruptedException | ExecutionException e) {
      throw new PartitionWriteException(e);
    }
  }

  private void writeOneToOne(final List<Block> blocksToWrite) throws ExecutionException, InterruptedException {
    final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), srcTaskIdx);

    // Write data.
    partitionManagerWorker.putBlocks(
        partitionId, blocksToWrite, channelDataPlacement, false);

    // Commit partition.
    partitionManagerWorker.commitPartition(
        partitionId, channelDataPlacement, Collections.emptyList(), srcVertexId);
  }

  private void writeBroadcast(final List<Block> blocksToWrite) throws ExecutionException, InterruptedException {
    writeOneToOne(blocksToWrite);
  }

  private void writeScatterGather(final List<Block> blocksToWrite) throws ExecutionException, InterruptedException {
    final int dstParallelism = dstVertex.getProperty(ExecutionProperty.Key.Parallelism);
    if (blocksToWrite.size() != dstParallelism) {
      throw new PartitionWriteException(
          new Throwable("The number of given blocks are not matched with the destination parallelism."));
    }

    // Then write each partition appropriately to the target data placement.
    final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), srcTaskIdx);
    // Write data.
    partitionManagerWorker.putBlocks(
        partitionId, blocksToWrite, channelDataPlacement, false);
    // Commit partition.
    partitionManagerWorker.commitPartition(
        partitionId, channelDataPlacement, Collections.emptyList(), srcVertexId);
  }

  /**
   * Writes blocks in a single partition and collects the size of each block.
   * This function will be called only when we need to split or recombine an output data from a task after it is stored
   * (e.g., dynamic data skew handling).
   * We extend the hash range with the factor {@link edu.snu.vortex.client.JobConf.HashRangeMultiplier} in advance
   * to prevent the extra deserialize - rehash - serialize process.
   * Each data of this partition having same key hash value will be collected as a single block.
   * This block will be the unit of retrieval and recombination of this partition.
   * Constraint: If a partition is written by this method, it have to be read by {@link InputReader#readDataInRange()}.
   * TODO #378: Elaborate block construction during data skew pass
   * TODO #428: DynOpt-clean up the metric collection flow
   *
   * @param blocksToWrite a list of the blocks to be written.
   * @throws ExecutionException      when fail to get results from futures.
   * @throws InterruptedException    when interrupted during getting results from futures.
   * @throws PartitionWriteException when fail to get the block size after write.
   */
  private void dataSkewWrite(final List<Block> blocksToWrite)
      throws ExecutionException, InterruptedException, PartitionWriteException {
    final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), srcTaskIdx);

    // Write data.
    final Optional<List<Long>> optionalBlockSize = partitionManagerWorker.putBlocks(
        partitionId, blocksToWrite, channelDataPlacement, false);
    if (optionalBlockSize.isPresent()) {
      // Commit partition.
      partitionManagerWorker.commitPartition(
          partitionId, channelDataPlacement, optionalBlockSize.get(), srcVertexId);
    } else {
      throw new PartitionWriteException(new Throwable("Cannot know the size of blocks"));
    }
  }
}
