/*
 * Copyright (C) 2018 Seoul National University
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
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import edu.snu.nemo.runtime.common.RuntimeIdManager;
import edu.snu.nemo.runtime.common.plan.RuntimeEdge;
import edu.snu.nemo.runtime.executor.data.BlockManagerWorker;
import edu.snu.nemo.runtime.executor.data.block.Block;
import edu.snu.nemo.runtime.executor.data.partitioner.*;

import java.util.*;

/**
 * Represents the output data transfer from a task.
 */
public final class OutputWriter extends DataTransfer implements AutoCloseable {
  private final RuntimeEdge<?> runtimeEdge;
  private final IRVertex dstIrVertex;
  private final DataStoreProperty.Value blockStoreValue;
  private final BlockManagerWorker blockManagerWorker;
  private final boolean nonDummyBlock;
  private final Block blockToWrite;
  private long writtenBytes;
  private Partitioner partitioner;

  /**
   * Constructor.
   *
   * @param hashRangeMultiplier the {@link edu.snu.nemo.conf.JobConf.HashRangeMultiplier}.
   * @param srcTaskId           the id of the source task.
   * @param dstIrVertex         the destination IR vertex.
   * @param runtimeEdge         the {@link RuntimeEdge}.
   * @param blockManagerWorker  the {@link BlockManagerWorker}.
   */
  OutputWriter(final int hashRangeMultiplier,
               final String srcTaskId,
               final IRVertex dstIrVertex,
               final RuntimeEdge<?> runtimeEdge,
               final BlockManagerWorker blockManagerWorker) {
    super(runtimeEdge.getId());
    this.runtimeEdge = runtimeEdge;
    this.dstIrVertex = dstIrVertex;
    this.blockManagerWorker = blockManagerWorker;
    this.blockStoreValue = runtimeEdge.getPropertyValue(DataStoreProperty.class).
        orElseThrow(() -> new RuntimeException("No data store property on the edge"));


    // Setup partitioner
    final int dstParallelism = dstIrVertex.getPropertyValue(ParallelismProperty.class).
        orElseThrow(() -> new RuntimeException("No parallelism property on the destination vertex"));
    final Optional<KeyExtractor> keyExtractor = runtimeEdge.getPropertyValue(KeyExtractorProperty.class);
    final PartitionerProperty.Value partitionerPropertyValue =
        runtimeEdge.getPropertyValue(PartitionerProperty.class).
            orElseThrow(() -> new RuntimeException("No partitioner property on the edge"));
    switch (partitionerPropertyValue) {
      case IntactPartitioner:
        this.partitioner = new IntactPartitioner();
        break;
      case HashPartitioner:
        this.partitioner = new HashPartitioner(dstParallelism, keyExtractor.
            orElseThrow(() -> new RuntimeException("No key extractor property on the edge")));
        break;
      case DataSkewHashPartitioner:
        this.partitioner = new DataSkewHashPartitioner(hashRangeMultiplier, dstParallelism, keyExtractor.
            orElseThrow(() -> new RuntimeException("No key extractor property on the edge")));
        break;
      case DedicatedKeyPerElementPartitioner:
        this.partitioner = new DedicatedKeyPerElementPartitioner();
        break;
      default:
        throw new UnsupportedPartitionerException(
            new Throwable("Partitioner " + partitionerPropertyValue + " is not supported."));
    }
    blockToWrite = blockManagerWorker.createBlock(
        RuntimeIdManager.generateBlockId(getId(), srcTaskId), blockStoreValue);
    final Optional<DuplicateEdgeGroupPropertyValue> duplicateDataProperty =
        runtimeEdge.getPropertyValue(DuplicateEdgeGroupProperty.class);
    nonDummyBlock = !duplicateDataProperty.isPresent()
        || duplicateDataProperty.get().getRepresentativeEdgeId().equals(runtimeEdge.getId())
        || duplicateDataProperty.get().getGroupSize() <= 1;
  }

  /**
   * Writes output element depending on the communication pattern of the edge.
   *
   * @param element the element to write.
   */
  public void write(final Object element) {
    if (nonDummyBlock) {
      blockToWrite.write(partitioner.partition(element), element);

      final DedicatedKeyPerElement dedicatedKeyPerElement =
          partitioner.getClass().getAnnotation(DedicatedKeyPerElement.class);
      if (dedicatedKeyPerElement != null) {
        blockToWrite.commitPartitions();
      }
    } // If else, does not need to write because the data is duplicated.
  }

  /**
   * Notifies that all writes for a block is end.
   * Further write about a committed block will throw an exception.
   */
  public void close() {
    // Commit block.
    final DataPersistenceProperty.Value persistence =
        runtimeEdge.getPropertyValue(DataPersistenceProperty.class).
            orElseThrow(() -> new RuntimeException("No data persistence property on the edge"));

    final boolean isDataSizeMetricCollectionEdge = Optional.of(MetricCollectionProperty.Value.DataSkewRuntimePass)
        .equals(runtimeEdge.getPropertyValue(MetricCollectionProperty.class));
    final Optional<Map<Integer, Long>> partitionSizeMap = blockToWrite.commit();
    // Return the total size of the committed block.
    if (partitionSizeMap.isPresent()) {
      long blockSizeTotal = 0;
      for (final long partitionSize : partitionSizeMap.get().values()) {
        blockSizeTotal += partitionSize;
      }
      this.writtenBytes = blockSizeTotal;
      blockManagerWorker.writeBlock(blockToWrite, blockStoreValue, isDataSizeMetricCollectionEdge,
          partitionSizeMap.get(), getExpectedRead(), persistence);
    } else {
      this.writtenBytes = -1; // no written bytes info.
      blockManagerWorker.writeBlock(blockToWrite, blockStoreValue, isDataSizeMetricCollectionEdge,
          Collections.emptyMap(), getExpectedRead(), persistence);
    }
  }

  /**
   * @return the total written bytes.
   */
  public Optional<Long> getWrittenBytes() {
    if (writtenBytes == -1) {
      return Optional.empty();
    } else {
      return Optional.of(writtenBytes);
    }
  }

  /**
   * Get the expected number of data read according to the communication pattern of the edge and
   * the parallelism of destination vertex.
   *
   * @return the expected number of data read.
   */
  private int getExpectedRead() {
    final Optional<DuplicateEdgeGroupPropertyValue> duplicateDataProperty =
        runtimeEdge.getPropertyValue(DuplicateEdgeGroupProperty.class);
    final int duplicatedDataMultiplier =
        duplicateDataProperty.isPresent() ? duplicateDataProperty.get().getGroupSize() : 1;
    final int readForABlock = CommunicationPatternProperty.Value.OneToOne.equals(
        runtimeEdge.getPropertyValue(CommunicationPatternProperty.class).orElseThrow(
            () -> new RuntimeException("No communication pattern on this edge.")))
        ? 1 : dstIrVertex.getPropertyValue(ParallelismProperty.class).orElseThrow(
            () -> new RuntimeException("No parallelism property on the destination vertex."));
    return readForABlock * duplicatedDataMultiplier;
  }
}
