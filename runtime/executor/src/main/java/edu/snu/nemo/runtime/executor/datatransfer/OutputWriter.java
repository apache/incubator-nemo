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
  private Optional<Long> writtenBytes;
  private final BlockManagerWorker blockManagerWorker;
  private final Partitioner partitioner;

  /**
   * Constructor.
   *
   * @param hashRangeMultiplier the {@link edu.snu.nemo.conf.JobConf.HashRangeMultiplier}.
   * @param srcTaskIdx          the index of the source task.
   * @param srcRuntimeVertexId  the ID of the source vertex.
   * @param dstIrVertex         the destination IR vertex.
   * @param runtimeEdge         the {@link RuntimeEdge}.
   * @param blockManagerWorker  the {@link BlockManagerWorker}.
   */
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
    this.writtenBytes = Optional.empty();

    // Setup partitioner
    final int dstParallelism = getDstParallelism();
    final KeyExtractor keyExtractor = runtimeEdge.getProperty(ExecutionProperty.Key.KeyExtractor);
    final PartitionerProperty.Value partitionerPropertyValue =
        runtimeEdge.getProperty(ExecutionProperty.Key.Partitioner);
    switch (partitionerPropertyValue) {
      case IntactPartitioner:
        this.partitioner = new IntactPartitioner();
        break;
      case HashPartitioner:
        this.partitioner = new HashPartitioner(dstParallelism, keyExtractor);
        break;
      case DataSkewHashPartitioner:
        this.partitioner = new DataSkewHashPartitioner(hashRangeMultiplier, dstParallelism, keyExtractor);
        break;
      default:
        throw new UnsupportedPartitionerException(
            new Throwable("Partitioner " + partitionerPropertyValue + " is not supported."));
    }
    blockManagerWorker.createBlock(blockId, blockStoreValue);
  }

  /**
   * Writes output data depending on the communication pattern of the edge.
   *
   * @param dataToWrite An iterable for the elements to be written.
   */
  public void write(final Iterable dataToWrite) {
    final DuplicateEdgeGroupPropertyValue duplicateDataProperty =
        runtimeEdge.getProperty(ExecutionProperty.Key.DuplicateEdgeGroup);
    if (duplicateDataProperty == null
        || duplicateDataProperty.getRepresentativeEdgeId().equals(runtimeEdge.getId())
        || duplicateDataProperty.getGroupSize() <= 1) {
      dataToWrite.forEach(element -> {
        blockManagerWorker.write(blockId, partitioner.partition(element), element, blockStoreValue);
      });
    } // If else, does not need to write because the data is duplicated.
  }

  /**
   * Notifies that all writes for a block is end.
   * Further write about a committed block will throw an exception.
   */
  public void close() {
    // Commit block.
    final UsedDataHandlingProperty.Value usedDataHandling =
        runtimeEdge.getProperty(ExecutionProperty.Key.UsedDataHandling);
    final DuplicateEdgeGroupPropertyValue duplicateDataProperty =
        runtimeEdge.getProperty(ExecutionProperty.Key.DuplicateEdgeGroup);
    final int multiplier = duplicateDataProperty == null ? 1 : duplicateDataProperty.getGroupSize();

    final boolean isDataSizeMetricCollectionEdge = MetricCollectionProperty.Value.DataSkewRuntimePass
        .equals(runtimeEdge.getProperty(ExecutionProperty.Key.MetricCollection));
    this.writtenBytes = blockManagerWorker.commitBlock(
        blockId, blockStoreValue, isDataSizeMetricCollectionEdge, srcVertexId,
        getDstParallelism() * multiplier, usedDataHandling);
  }

  /**
   * @return the total written bytes.
   */
  public Optional<Long> getWrittenBytes() {
    return writtenBytes;
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
