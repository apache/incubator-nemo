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
import edu.snu.vortex.runtime.common.RuntimeAttribute;
import edu.snu.vortex.runtime.common.RuntimeAttributeMap;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.exception.UnsupportedCommPatternException;
import edu.snu.vortex.runtime.exception.UnsupportedPartitionerException;
import edu.snu.vortex.runtime.executor.block.BlockManagerWorker;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Represents the output data transfer from a task.
 */
public final class OutputWriter extends DataTransfer {
  /**
   * Attributes that specify how we should write the output.
   */
  private final RuntimeAttributeMap edgeAttributes;
  private final RuntimeAttributeMap dstVertexAttributes;

  /**
   * The Block Manager Worker.
   */
  private final BlockManagerWorker blockManagerWorker;

  private final String edgeId;
  private final int srcTaskindex;

  public OutputWriter(final String edgeId,
                      final int srcTaskIndex,
                      final RuntimeAttributeMap dstVertexAttributes,
                      final RuntimeAttributeMap edgeAttributes,
                      final BlockManagerWorker blockManagerWorker) {
    super(edgeId);
    this.edgeAttributes = edgeAttributes;
    this.dstVertexAttributes = dstVertexAttributes;
    this.blockManagerWorker = blockManagerWorker;
    this.edgeId = edgeId;
    this.srcTaskindex = srcTaskIndex;
  }

  /**
   * Writes output data depending on the communication pattern of the edge.
   * @param dataToWrite An iterable for the elements to be written.
   */
  public void write(final Iterable<Element> dataToWrite) {
    switch (edgeAttributes.get(RuntimeAttribute.Key.CommPattern)) {
    case OneToOne:
      writeOneToOne(dataToWrite);
      break;
    case Broadcast:
      writeBroadcast(dataToWrite);
      break;
    case ScatterGather:
      writeScatterGather(dataToWrite);
      break;
    default:
      throw new UnsupportedCommPatternException(new Exception("Communication pattern not supported"));
    }
  }

  private void writeOneToOne(final Iterable<Element> dataToWrite) {
    final String blockId = RuntimeIdGenerator.generateBlockId(edgeId, srcTaskindex);
    blockManagerWorker.putBlock(blockId, dataToWrite, edgeAttributes.get(RuntimeAttribute.Key.BlockStore));
  }

  private void writeBroadcast(final Iterable<Element> dataToWrite) {
    final String blockId = RuntimeIdGenerator.generateBlockId(edgeId, srcTaskindex);
    blockManagerWorker.putBlock(blockId, dataToWrite, edgeAttributes.get(RuntimeAttribute.Key.BlockStore));
  }

  private void writeScatterGather(final Iterable<Element> dataToWrite) {
    final RuntimeAttribute partition = edgeAttributes.get(RuntimeAttribute.Key.Partition);
    switch (partition) {
    case Hash:
      final int dstParallelism = dstVertexAttributes.get(RuntimeAttribute.IntegerKey.Parallelism);

      // First partition the data to write,
      final List<List<Element>> partitionedOutputList = new ArrayList<>(dstParallelism);
      IntStream.range(0, dstParallelism).forEach(partitionIdx -> partitionedOutputList.add(new ArrayList<>()));
      dataToWrite.forEach(element -> {
        // Hash the data by its key, and "modulo" the number of destination tasks.
        final int dstIdx = Math.abs(element.getKey().hashCode() % dstParallelism);
        partitionedOutputList.get(dstIdx).add(element);
      });

      // Then write each partition appropriately to the target data placement.
      IntStream.range(0, dstParallelism).forEach(partitionIdx -> {
        // Give each partition its own block id
        final String blockId = RuntimeIdGenerator.generateBlockId(edgeId, srcTaskindex, partitionIdx);
        blockManagerWorker.putBlock(blockId,
            partitionedOutputList.get(partitionIdx),
            edgeAttributes.get(RuntimeAttribute.Key.BlockStore));
      });
      break;
    case Range:
    default:
      throw new UnsupportedPartitionerException(new Exception(partition + " partitioning not yet supported"));
    }
  }
}
