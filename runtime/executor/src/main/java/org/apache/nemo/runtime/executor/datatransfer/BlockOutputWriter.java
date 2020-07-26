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
import org.apache.nemo.runtime.executor.data.MemoryManager;
import org.apache.nemo.runtime.executor.data.SizeTrackingVector;
import org.apache.nemo.runtime.executor.data.block.Block;
//import org.apache.nemo.runtime.executor.data.stores.BlockStore;
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
  private Optional<Block> blockToWrite;
//  private final Optional<Block> potentialSpilledBlocktoWrite;
  private final boolean nonDummyBlock;

  private long writtenBytes;

  //dongjoo
  private final MemoryManager memoryManager;
  private SizeTrackingVector sizeTrackingVector;
  private boolean outputSpilled;
  private String srcTaskId;
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
    this.srcTaskId = srcTaskId;
    this.blockStoreValue = runtimeEdge.getPropertyValue(DataStoreProperty.class)
      .orElseThrow(() -> new RuntimeException("No data store property on the edge"));
    blockToWrite = blockManagerWorker.createBlock(
      RuntimeIdManager.generateBlockId(runtimeEdge.getId(), srcTaskId), blockStoreValue);
    // potential file block to spill to
//    potentialSpilledBlocktoWrite = blockManagerWorker.createBlock(
//      RuntimeIdManager.generateBlockId(runtimeEdge.getId() + "spilled", srcTaskId),
//      DataStoreProperty.Value.LOCAL_FILE_STORE);
//
//    if (blockStoreValue == DataStoreProperty.Value.MEMORY_FILE_STORE
//      || blockStoreValue == DataStoreProperty.Value.SERIALIZED_MEMORY_FILE_STORE) {
//      potentialSpilledBlocktoWrite = blockManagerWorker.createBlock(
//        RuntimeIdManager.generateBlockId(runtimeEdge.getId() + "spilled", srcTaskId),
//        DataStoreProperty.Value.LOCAL_FILE_STORE);
//    } else {
//      potentialSpilledBlocktoWrite = null; // null is bad, how to change?
//    }

//    LOG.info("dongjoo, BlockOutputWriter constructor, stageEdge {}, runtimeEdge {}, blockToWrite {}",
//      stageEdge, runtimeEdge, blockToWrite);
    final Optional<DuplicateEdgeGroupPropertyValue> duplicateDataProperty =
      runtimeEdge.getPropertyValue(DuplicateEdgeGroupProperty.class);
    nonDummyBlock = !duplicateDataProperty.isPresent()
      || duplicateDataProperty.get().getRepresentativeEdgeId().equals(runtimeEdge.getId())
      || duplicateDataProperty.get().getGroupSize() <= 1;

    //dongjoo
    this.memoryManager = memoryManager;
    this.sizeTrackingVector = new SizeTrackingVector();
    this.outputSpilled = false;

  }


  @Override
  public void write(final Object element) {
    LOG.info("dongjoo BlockOutputWriter write element {}", element);
//    LOG.info("type of partitioner, key: {} and type of block {}, block id {}",
//      partitioner.partition(element), blockStoreValue, blockToWrite.get().getId());
    if (nonDummyBlock) {
      // if blockStore is caching to Memory, write to temporary SizeTrackingVector to ensure OOM doesn't occur,
      // actual "writing" will occur in { close }
      if (blockStoreValue == DataStoreProperty.Value.MEMORY_FILE_STORE
        || blockStoreValue == DataStoreProperty.Value.SERIALIZED_MEMORY_FILE_STORE) {
//        LOG.info("only for memory store append to STV");
        sizeTrackingVector.append(element);
//        LOG.info("sizeTrackingVector size: {}", sizeTrackingVector.estimateSize());
//        memoryManager.acquireStorageMemory(blockToWrite.getId(), SizeEstimator.estimate(element));
      } else { // if blockToWrite.isPresent()
        // original logic
        // dongjoo: partioner.partition returns key of the partition
//        LOG.info("not a memory store block, just write");
        blockToWrite.get().write(partitioner.partition(element), element);
      }
      // other logic, not write or append to SizeTrackingVector, common operation?
      final DedicatedKeyPerElement dedicatedKeyPerElement =
        partitioner.getClass().getAnnotation(DedicatedKeyPerElement.class);
      if (dedicatedKeyPerElement != null) {
        LOG.info("COMMITPARTITIONS CALLED BECAUSE OF DEDICATED KEY");
        blockToWrite.get().commitPartitions();
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
//    LOG.info("close called on block {}, potential {}, blockstorevlue {}",
//      blockToWrite.get().getId(), potentialSpilledBlocktoWrite.get().getId(), blockStoreValue);
//    LOG.info("close called on block {}, potential {}, blockstorevlue {}",
//      blockToWrite.get().getId(), "none", blockStoreValue);
    // for MemoryStore blocks
    if (blockStoreValue == DataStoreProperty.Value.MEMORY_FILE_STORE
      || blockStoreValue == DataStoreProperty.Value.SERIALIZED_MEMORY_FILE_STORE) {
      LOG.info("BOW close called, iterating over STV and writing to block");
//      if (memoryManager.acquireStorageMemory(blockToWrite.get().getId(), sizeTrackingVector.estimateSize())) {
      if (memoryManager.acquireStorageMemory(RuntimeIdManager.generateBlockId(runtimeEdge.getId(), srcTaskId),
        sizeTrackingVector.estimateSize())) {
        blockToWrite = blockManagerWorker.createAndFinalizeStoreProperty(
          RuntimeIdManager.generateBlockId(runtimeEdge.getId(), srcTaskId), blockStoreValue);
        LOG.info("IMPORTANT! enough size in memeory to write block {} to memory", blockToWrite.get().getId());
        sizeTrackingVector.forEach(element -> blockToWrite.get().write(partitioner.partition(element), element));
        final Optional<Map<Integer, Long>> newpartitionSizeMap =
          blockToWrite.get().commit();
        // need common operation after commit for partition size and writeBlock
        final DataPersistenceProperty.Value persistence = (DataPersistenceProperty.Value) runtimeEdge
          .getPropertyValue(DataPersistenceProperty.class).orElseThrow(IllegalStateException::new);
        blockManagerWorker.writeBlock(blockToWrite.get(), blockStoreValue, getExpectedRead(), persistence);
      } else { // block does not fit in memory
        this.outputSpilled = true;
        LOG.info("outputSpilled is {}", this.outputSpilled);
        blockToWrite = blockManagerWorker.createBlock(
          RuntimeIdManager.generateBlockId(runtimeEdge.getId(), srcTaskId), DataStoreProperty.Value.LOCAL_FILE_STORE);
        LOG.info("Not enough storage memory, remaing StoragePool Memory: {}, size of block is {}, blockID: {}",
          memoryManager.getRemainingStorageMemory(), sizeTrackingVector.estimateSize(), blockToWrite.get().getId());
        sizeTrackingVector.forEach(element ->
          blockToWrite.get().write(partitioner.partition(element), element));
        blockManagerWorker.putSpilledBlock(blockToWrite.get(), blockToWrite.get());
        // commit the file block
        final DataPersistenceProperty.Value persistence = DataPersistenceProperty.Value.KEEP; // hardcode as keep?
        final Optional<Map<Integer, Long>> newpartitionSizeMap =
          blockToWrite.get().commit();

        /// delete sizeTrackingVector and release memory back into StoragePool
//        this.memoryManager.releaseStorageMemory(this.sizeTrackingVector.estimateSize());
//        this.sizeTrackingVector.clear();

        /// Return the total size of the committed block.
        if (newpartitionSizeMap.isPresent()) {
          long blockSizeTotal = 0;
          for (final long partitionSize : newpartitionSizeMap.get().values()) {
            blockSizeTotal += partitionSize;
          }
          writtenBytes = blockSizeTotal;
        } else {
          writtenBytes = -1; // no written bytes info.
        }
        // commit the spilled block
        blockManagerWorker.writeBlock(blockToWrite.get(),
          DataStoreProperty.Value.LOCAL_FILE_STORE, getExpectedRead(), persistence);
      }
    } else { //caching does not need to be considered since the block is a local file store block
      // Commit block.
      LOG.info("dongjoo BlockOutputWriter close block, closing {} id {}", blockToWrite, blockToWrite.get().getId());
      final DataPersistenceProperty.Value persistence = (DataPersistenceProperty.Value) runtimeEdge
        .getPropertyValue(DataPersistenceProperty.class).orElseThrow(IllegalStateException::new);

      final Optional<Map<Integer, Long>> partitionSizeMap = blockToWrite.get().commit();
      // Return the total size of the committed block.
      if (partitionSizeMap.isPresent()) {
        long blockSizeTotal = 0;
        for (final long partitionSize : partitionSizeMap.get().values()) {
          blockSizeTotal += partitionSize;
        }
        writtenBytes = Math.max(blockSizeTotal, writtenBytes);
      } else {
        writtenBytes = -1; // no written bytes info.
      }
      blockManagerWorker.writeBlock(blockToWrite.get(), blockStoreValue, getExpectedRead(), persistence);
    }
    if (!this.outputSpilled) {
//      blockManagerWorker.removeBlock(potentialSpilledBlocktoWrite.getId(), DataStoreProperty.Value.LOCAL_FILE_STORE);
//      potentialSpilledBlocktoWrite
      String a = "do nothing";
    }
    LOG.info("end of close, blockid {}, block contents {}, datastoreProperty {}",
      blockToWrite.get().getId(), blockToWrite.get(), blockStoreValue);
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
