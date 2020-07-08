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
package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.partitioner.DedicatedKeyPerElement;
import org.apache.nemo.common.partitioner.Partitioner;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.executor.data.BlockManagerWorker;
//import org.apache.nemo.runtime.executor.data.MemoryManager;
import org.apache.nemo.runtime.executor.data.MemoryManager;
import org.apache.nemo.runtime.executor.data.SizeEstimator;
import org.apache.nemo.runtime.executor.data.SizeTrackingVector;
import org.apache.nemo.runtime.executor.data.block.Block;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

/**
 * Represents the output data transfer from a task.
 */
public final class BlockOutputWriter implements OutputWriter {
  private static final Logger LOG = LoggerFactory.getLogger(BlockOutputWriter.class.getName());

  private final RuntimeEdge<?> runtimeEdge;
  private final IRVertex dstIrVertex;
  private final Partitioner partitioner;

  private final DataStoreProperty.Value blockStoreValue;
  private final BlockManagerWorker blockManagerWorker;
  private final Block blockToWrite;
  private final boolean nonDummyBlock;

  private long writtenBytes;

  //dongjoo
  private final MemoryManager memoryManager;
  private SizeTrackingVector sizeTrackingVector;

  /**
   * Constructor.
   *
   * @param srcTaskId          the id of the source task.
   * @param dstIrVertex        the destination IR vertex.
   * @param runtimeEdge        the {@link RuntimeEdge}.
   * @param blockManagerWorker the {@link BlockManagerWorker}.
   */
  BlockOutputWriter(final String srcTaskId,
                    final IRVertex dstIrVertex,
                    final RuntimeEdge<?> runtimeEdge,
                    final BlockManagerWorker blockManagerWorker,
                    final MemoryManager memoryManager) {
    final StageEdge stageEdge = (StageEdge) runtimeEdge;
    this.runtimeEdge = runtimeEdge;
    this.dstIrVertex = dstIrVertex;
    this.partitioner = Partitioner
      .getPartitioner(stageEdge.getExecutionProperties(), stageEdge.getDstIRVertex().getExecutionProperties());
    this.blockManagerWorker = blockManagerWorker;
    this.blockStoreValue = runtimeEdge.getPropertyValue(DataStoreProperty.class)
      .orElseThrow(() -> new RuntimeException("No data store property on the edge"));
    blockToWrite = blockManagerWorker.createBlock(
      RuntimeIdManager.generateBlockId(runtimeEdge.getId(), srcTaskId), blockStoreValue);
    LOG.info("dongjoo, BlockOutputWriter constructor, stageEdge {}, runtimeEdge {}, blockToWrite {}",
      stageEdge, runtimeEdge, blockToWrite);
    final Optional<DuplicateEdgeGroupPropertyValue> duplicateDataProperty =
      runtimeEdge.getPropertyValue(DuplicateEdgeGroupProperty.class);
    nonDummyBlock = !duplicateDataProperty.isPresent()
      || duplicateDataProperty.get().getRepresentativeEdgeId().equals(runtimeEdge.getId())
      || duplicateDataProperty.get().getGroupSize() <= 1;

    //dongjoo
    this.memoryManager = memoryManager;
    this.sizeTrackingVector = new SizeTrackingVector();

  }


  @Override
  public void write(final Object element) {
    if (nonDummyBlock) {
      LOG.info("BlockOutPutWriter write, blockid {},blocktoWrite {}",
        blockToWrite.getId(), blockToWrite);
      LOG.info("written element {}", element);

      LOG.info("estimated size of element is : {}", SizeEstimator.estimate(element));
      LOG.info("appending to sizeTrackingVector");
      sizeTrackingVector.append(element);
      LOG.info("sizeTrackingVector size: {}", sizeTrackingVector.estimateSize());
      LOG.info("STV INFO {}", sizeTrackingVector.getInfo());
      memoryManager.acquireStorageMemory(SizeEstimator.estimate(element));
      LOG.info("unique id of memory manager  {}", memoryManager.getUniqueId());
      blockToWrite.write(partitioner.partition(element), element);

      final DedicatedKeyPerElement dedicatedKeyPerElement =
        partitioner.getClass().getAnnotation(DedicatedKeyPerElement.class);
      if (dedicatedKeyPerElement != null) {
        blockToWrite.commitPartitions();
      }
    } // If else, does not need to write because the data is duplicated.
  }

  @Override
  public void writeWatermark(final Watermark watermark) {
    // do nothing
  }

  /**
   * Notifies that all writes for a block have ended.
   * Further write to a committed block will throw an exception.
   */
  @Override
  public void close() {
    // Commit block.
    LOG.info("dongjoo BlockOutputWriter close block, closing {} id {}", blockToWrite, blockToWrite.getId());
    final DataPersistenceProperty.Value persistence = (DataPersistenceProperty.Value) runtimeEdge
      .getPropertyValue(DataPersistenceProperty.class).orElseThrow(IllegalStateException::new);

    final Optional<Map<Integer, Long>> partitionSizeMap = blockToWrite.commit();
    // Return the total size of the committed block.
    if (partitionSizeMap.isPresent()) {
      long blockSizeTotal = 0;
      for (final long partitionSize : partitionSizeMap.get().values()) {
        blockSizeTotal += partitionSize;
      }
      writtenBytes = blockSizeTotal;
    } else {
      writtenBytes = -1; // no written bytes info.
    }
    blockManagerWorker.writeBlock(blockToWrite, blockStoreValue, getExpectedRead(), persistence);
  }

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
    final int readForABlock = CommunicationPatternProperty.Value.ONE_TO_ONE.equals(
      runtimeEdge.getPropertyValue(CommunicationPatternProperty.class).orElseThrow(
        () -> new RuntimeException("No communication pattern on this edge.")))
      ? 1 : dstIrVertex.getPropertyValue(ParallelismProperty.class).orElseThrow(
      () -> new RuntimeException("No parallelism property on the destination vertex."));
    return readForABlock * duplicatedDataMultiplier;
  }
}
