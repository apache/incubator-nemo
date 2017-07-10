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
import edu.snu.vortex.compiler.ir.attribute.Attribute;
import edu.snu.vortex.runtime.common.RuntimeIdGenerator;
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex;
import edu.snu.vortex.runtime.exception.UnsupportedCommPatternException;
import edu.snu.vortex.runtime.exception.UnsupportedPartitionerException;
import edu.snu.vortex.runtime.executor.data.PartitionManagerWorker;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Represents the output data transfer from a task.
 */
public final class OutputWriter extends DataTransfer {
  private final int srcTaskIdx;
  private final RuntimeEdge runtimeEdge;
  private final RuntimeVertex dstRuntimeVertex;

  /**
   * The Block Manager Worker.
   */
  private final PartitionManagerWorker partitionManagerWorker;

  public OutputWriter(final int srcTaskIdx,
                      final RuntimeVertex dstRuntimeVertex,
                      final RuntimeEdge runtimeEdge,
                      final PartitionManagerWorker partitionManagerWorker) {
    super(runtimeEdge.getId());
    this.runtimeEdge = runtimeEdge;
    this.dstRuntimeVertex = dstRuntimeVertex;
    this.partitionManagerWorker = partitionManagerWorker;
    this.srcTaskIdx = srcTaskIdx;
  }

  /**
   * Writes output data depending on the communication pattern of the edge.
   * @param dataToWrite An iterable for the elements to be written.
   */
  public void write(final Iterable<Element> dataToWrite) {
    switch (runtimeEdge.getEdgeAttributes().get(Attribute.Key.CommunicationPattern)) {
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
    final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), srcTaskIdx);
    partitionManagerWorker.putPartition(partitionId, dataToWrite,
        runtimeEdge.getEdgeAttributes().get(Attribute.Key.ChannelDataPlacement));
  }

  private void writeBroadcast(final Iterable<Element> dataToWrite) {
    final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), srcTaskIdx);
    partitionManagerWorker.putPartition(partitionId, dataToWrite,
        runtimeEdge.getEdgeAttributes().get(Attribute.Key.ChannelDataPlacement));
  }

  private void writeScatterGather(final Iterable<Element> dataToWrite) {
    final Attribute partition = runtimeEdge.getEdgeAttributes().get(Attribute.Key.Partitioning);
    switch (partition) {
    case Hash:
      final int dstParallelism = dstRuntimeVertex.getVertexAttributes().get(Attribute.IntegerKey.Parallelism);

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
        // Give each partition its own partition id
        final String partitionId = RuntimeIdGenerator.generatePartitionId(getId(), srcTaskIdx, partitionIdx);
        partitionManagerWorker.putPartition(partitionId,
            partitionedOutputList.get(partitionIdx),
            runtimeEdge.getEdgeAttributes().get(Attribute.Key.ChannelDataPlacement));
      });
      break;
    case Range:
    default:
      throw new UnsupportedPartitionerException(new Exception(partition + " partitioning not yet supported"));
    }
  }
}
