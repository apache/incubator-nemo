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

import edu.snu.vortex.common.Pair;
import edu.snu.vortex.compiler.ir.Element;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.ir.executionproperty.edge.WriteOptimizationProperty;
import edu.snu.vortex.compiler.optimizer.pass.runtime.DataSkewRuntimePass;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.exception.PartitionWriteException;
import edu.snu.vortex.runtime.exception.UnsupportedCommPatternException;
import edu.snu.vortex.runtime.exception.UnsupportedMethodException;
import edu.snu.vortex.runtime.exception.UnsupportedPartitionerException;
import edu.snu.vortex.runtime.executor.data.Block;
import edu.snu.vortex.runtime.executor.data.PartitionManagerWorker;
import edu.snu.vortex.runtime.executor.data.PartitionStore;
import edu.snu.vortex.runtime.executor.datatransfer.data_communication_pattern.Broadcast;
import edu.snu.vortex.runtime.executor.datatransfer.data_communication_pattern.OneToOne;
import edu.snu.vortex.runtime.executor.datatransfer.data_communication_pattern.ScatterGather;
import edu.snu.vortex.runtime.executor.datatransfer.partitioning.Hash;
import edu.snu.vortex.runtime.executor.datatransfer.partitioning.Partitioning;
import edu.snu.vortex.runtime.executor.datatransfer.partitioning.Range;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

/**
 * Represents the output data transfer from a task.
 */
public final class OutputWriter extends DataTransfer {
  private final int hashRangeMultiplier;
  private final int srcTaskIdx;
  private final RuntimeEdge<?> runtimeEdge;
  private final String srcVertexId;
  private final IRVertex dstVertex;
  private final Class<? extends PartitionStore> channelDataPlacement;

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
    this.hashRangeMultiplier = hashRangeMultiplier;
    this.runtimeEdge = runtimeEdge;
    this.srcVertexId = srcRuntimeVertexId;
    this.dstVertex = dstRuntimeVertex;
    this.partitionManagerWorker = partitionManagerWorker;
    this.srcTaskIdx = srcTaskIdx;
    this.channelDataPlacement = runtimeEdge.getProperty(ExecutionProperty.Key.DataStore);
  }

  /**
   * Writes output data depending on the communication pattern of the edge.
   *
   * @param dataToWrite An iterable for the elements to be written.
   */
  public void write(final Iterable<Element> dataToWrite) {
    final Boolean isDataSizeMetricCollectionEdge = DataSkewRuntimePass.class
        .equals(runtimeEdge.getProperty(ExecutionProperty.Key.MetricCollection));
    final String writeOptAtt = runtimeEdge.getProperty(ExecutionProperty.Key.WriteOptimization);
    final Boolean isIFileWriteEdge =
        writeOptAtt != null && writeOptAtt.equals(WriteOptimizationProperty.IFILE_WRITE);
    if (writeOptAtt != null && !writeOptAtt.equals(WriteOptimizationProperty.IFILE_WRITE)) {
      throw new UnsupportedMethodException("Unsupported write optimization.");
    }

    // TODO #463: Support incremental write.
    try {
      switch ((runtimeEdge.<Class>getProperty(ExecutionProperty.Key.DataCommunicationPattern)).getSimpleName()) {
        case OneToOne.SIMPLE_NAME:
          writeOneToOne(dataToWrite);
          break;
        case Broadcast.SIMPLE_NAME:
          writeBroadcast(dataToWrite);
          break;
        case ScatterGather.SIMPLE_NAME:
          // If the dynamic optimization which detects data skew is enabled, sort the data and write it.
          if (isDataSizeMetricCollectionEdge) {
            hashAndWrite(dataToWrite);
          } else if (isIFileWriteEdge) {
            writeIFile(dataToWrite);
          } else {
            writeScatterGather(dataToWrite);
          }
          break;
        default:
          throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
      }
    } catch (final InterruptedException | ExecutionException e) {
      throw new PartitionWriteException(e);
    }
  }

  private void writeOneToOne(final Iterable<Element> dataToWrite) throws ExecutionException, InterruptedException {
    final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), srcTaskIdx);
    final Block blockToWrite = new Block(dataToWrite);

    // Write data.
    partitionManagerWorker.putBlocks(
        partitionId, Collections.singleton(blockToWrite), channelDataPlacement, false);

    // Commit partition.
    partitionManagerWorker.commitPartition(
        partitionId, channelDataPlacement, Collections.emptyList(), srcVertexId, srcTaskIdx);
  }

  private void writeBroadcast(final Iterable<Element> dataToWrite) throws ExecutionException, InterruptedException {
    writeOneToOne(dataToWrite);
  }

  private void writeScatterGather(final Iterable<Element> dataToWrite) throws ExecutionException, InterruptedException {
    final Class<? extends Partitioning> partition =
        runtimeEdge.getProperty(ExecutionProperty.Key.Partitioning);
    switch (partition.getSimpleName()) {
      case Hash.SIMPLE_NAME:
        final int dstParallelism = dstVertex.getProperty(ExecutionProperty.Key.Parallelism);

        // First partition the data to write,
        final List<List<Element>> partitionedOutputList = new ArrayList<>(dstParallelism);
        IntStream.range(0, dstParallelism).forEach(partitionIdx -> partitionedOutputList.add(new ArrayList<>()));
        dataToWrite.forEach(element -> {
          // Hash the data by its key, and "modulo" the number of destination tasks.
          final int dstIdx = Math.abs(element.getKey().hashCode() % dstParallelism);
          partitionedOutputList.get(dstIdx).add(element);
        });

        // Then write each partition appropriately to the target data placement.
        final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), srcTaskIdx);
        final List<Block> blocksToWrite = new ArrayList<>(dstParallelism);
        for (int dstIdx = 0; dstIdx < dstParallelism; dstIdx++) {
          // Give each partition its own partition id
          blocksToWrite.add(new Block(dstIdx, partitionedOutputList.get(dstIdx)));
        }

        // Write data.
        partitionManagerWorker.putBlocks(
            partitionId, blocksToWrite, channelDataPlacement, false);

        // Commit partition.
        partitionManagerWorker.commitPartition(
            partitionId, channelDataPlacement, Collections.emptyList(), srcVertexId, srcTaskIdx);
        break;
      case Range.SIMPLE_NAME:
      default:
        throw new UnsupportedPartitionerException(new Exception(partition + " partitioning not yet supported"));
    }
  }

  /**
   * Hashes an output according to the hash value and writes it as a single partition.
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
   * @param dataToWrite an iterable for the elements to be written.
   * @throws ExecutionException      when fail to get results from futures.
   * @throws InterruptedException    when interrupted during getting results from futures.
   * @throws PartitionWriteException when fail to get the block size after write.
   */
  private void hashAndWrite(final Iterable<Element> dataToWrite)
      throws ExecutionException, InterruptedException, PartitionWriteException {
    final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), srcTaskIdx);
    final int dstParallelism = dstVertex.getProperty(ExecutionProperty.Key.Parallelism);
    // For this hash range, please check the description of HashRangeMultiplier
    final int hashRange = hashRangeMultiplier * dstParallelism;

    // Separate the data into blocks according to the hash of their key.
    final List<List<Element>> blockDataList = new ArrayList<>(hashRange);
    IntStream.range(0, hashRange).forEach(hashVal -> blockDataList.add(new ArrayList<>()));
    dataToWrite.forEach(element -> {
      // Hash the data by its key, and "modulo" by the hash range.
      final int hashVal = Math.abs(element.getKey().hashCode() % hashRange);
      blockDataList.get(hashVal).add(element);
    });
    final List<Block> blockList = new ArrayList<>(hashRange);
    for (int hashIdx = 0; hashIdx < hashRange; hashIdx++) {
      blockList.add(new Block(hashIdx, blockDataList.get(hashIdx)));
    }

    // Write data.
    final Optional<List<Long>> optionalBlockSize = partitionManagerWorker.putBlocks(
        partitionId, blockList, channelDataPlacement, false);
    if (optionalBlockSize.isPresent()) {
      // Commit partition.
      partitionManagerWorker.commitPartition(
          partitionId, channelDataPlacement, optionalBlockSize.get(), srcVertexId, srcTaskIdx);
    } else {
      throw new PartitionWriteException(new Throwable("Cannot know the size of blocks"));
    }
  }

  /**
   * Hashes an output according to the hash value and constructs I-Files with the hashed data blocks.
   * Each destination task will have a single I-File to read regardless of the input parallelism.
   * To prevent the extra sort process in the source task and deserialize - merge process in the destination task,
   * we hash the data into blocks and make the blocks as the unit of write and retrieval.
   * Constraint: If a partition is written by this method, it have to be read by {@link InputReader#readIFile()}.
   * Constraint: If the store to write is not a {@link edu.snu.vortex.runtime.executor.data.RemoteFileStore},
   *             all destination tasks for each I-File (partition) have to be scheduled in a single executor.
   * TODO #378: Elaborate block construction during data skew pass
   *
   * @param dataToWrite an iterable for the elements to be written.
   * @throws ExecutionException   when fail to get results from futures.
   * @throws InterruptedException when interrupted during getting results from futures.
   */
  private void writeIFile(final Iterable<Element> dataToWrite) throws ExecutionException, InterruptedException {
    final int dstParallelism = dstVertex.getProperty(ExecutionProperty.Key.Parallelism);
    // For this hash range, please check the description of HashRangeMultiplier
    final int hashRange = hashRangeMultiplier * dstParallelism;
    final List<List<Pair<Integer, Iterable<Element>>>> outputList = new ArrayList<>(dstParallelism);
    // Create data blocks for each I-File.
    IntStream.range(0, dstParallelism).forEach(dstIdx -> {
      final List<Pair<Integer, Iterable<Element>>> outputBlockList = new ArrayList<>(hashRangeMultiplier);
      IntStream.range(0, hashRangeMultiplier).forEach(hashValRemainder -> {
        final int hashVal = hashRangeMultiplier * dstIdx + hashValRemainder;
        outputBlockList.add(Pair.of(hashVal, new ArrayList<>()));
      });
      outputList.add(outputBlockList);
    });

    // Assigns data to the corresponding hashed block.
    dataToWrite.forEach(element -> {
      final int hashVal = Math.abs(element.getKey().hashCode() % hashRange);
      final int dstIdx = hashVal / hashRangeMultiplier;
      final int blockIdx = Math.abs(hashVal % hashRangeMultiplier);
      ((List) outputList.get(dstIdx).get(blockIdx).right()).add(element);
    });

    // Then append each blocks to corresponding partition appropriately.
    for (int dstIdx = 0; dstIdx < dstParallelism; dstIdx++) {
      final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), dstIdx);
      final List<Block> blockList = new ArrayList<>(hashRange);
      for (int hashIdx = 0; hashIdx < hashRangeMultiplier; hashIdx++) {
        final Pair<Integer, Iterable<Element>> hashValAndData = outputList.get(dstIdx).get(hashIdx);
        blockList.add(new Block(hashValAndData.left(), hashValAndData.right()));
      }

      // Write data.
      partitionManagerWorker.putBlocks(
          partitionId, blockList, channelDataPlacement, false);

      // Commit partition.
      partitionManagerWorker.commitPartition(
          partitionId, channelDataPlacement, Collections.emptyList(), srcVertexId, srcTaskIdx);
    }
  }
}
