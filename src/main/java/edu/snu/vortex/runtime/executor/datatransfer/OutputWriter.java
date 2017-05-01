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
import edu.snu.vortex.runtime.common.plan.RuntimeEdge;
import edu.snu.vortex.runtime.common.plan.logical.RuntimeVertex;
import edu.snu.vortex.runtime.common.plan.physical.Task;
import edu.snu.vortex.runtime.exception.UnsupportedCommPatternException;
import edu.snu.vortex.runtime.exception.UnsupportedPartitionerException;
import edu.snu.vortex.runtime.executor.dataplacement.DataPlacement;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Represents the output data transfer from a task.
 */
public final class OutputWriter extends DataTransfer {

  /**
   * The task that writes the data.
   */
  private final Task srcTask;

  /**
   * The {@link RuntimeVertex} where the output data is transferred to.
   */
  private final RuntimeVertex dstRuntimeVertex;

  /**
   * The {@link RuntimeEdge} that connects the srcTask to the tasks belonging to dstRuntimeVertex.
   */
  private final RuntimeEdge runtimeEdge;

  /**
   * Represents where the output data is placed.
   */
  private final DataPlacement dataPlacement;

  public OutputWriter(final Task srcTask,
                      final RuntimeVertex dstRuntimeVertex,
                      final RuntimeEdge runtimeEdge,
                      final DataPlacement dataPlacement) {
    super(runtimeEdge.getRuntimeEdgeId());
    this.srcTask = srcTask;
    this.dstRuntimeVertex = dstRuntimeVertex;
    this.runtimeEdge = runtimeEdge;
    this.dataPlacement = dataPlacement;
  }

  /**
   * Writes output data depending on the communication pattern of the dstRuntimeVertex.
   * @param dataToWrite An iterable for the elements to be written.
   */
  public void write(final Iterable<Element> dataToWrite) {
    switch (dstRuntimeVertex.getVertexAttributes().get(RuntimeAttribute.Key.CommPattern)) {
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
    dataPlacement.put(runtimeEdge.getRuntimeEdgeId(), srcTask.getIndex(), dataToWrite);
  }

  private void writeBroadcast(final Iterable<Element> dataToWrite) {
    dataPlacement.put(runtimeEdge.getRuntimeEdgeId(), srcTask.getIndex(), dataToWrite);
  }

  private void writeScatterGather(final Iterable<Element> dataToWrite) {
    final RuntimeAttribute partitioner = runtimeEdge.getEdgeAttributes().get(RuntimeAttribute.Key.Partition);
    final int dstParallelism = dstRuntimeVertex.getVertexAttributes().get(RuntimeAttribute.IntegerKey.Parallelism);

    switch (partitioner) {
    case Hash:
      // First partition the data to write,
      final List<List<Element>> partitionedOutputList = new ArrayList<>(dstParallelism);
      IntStream.range(0, dstParallelism).forEach(partitionIdx -> partitionedOutputList.add(new ArrayList<>()));
      dataToWrite.forEach(element -> {
        // Hash the data by its key, and "modulo" the number of destination tasks.
        final int dstIdx = Math.abs(element.getKey().hashCode() % dstParallelism);
        partitionedOutputList.get(dstIdx).add(element);
      });

      // Then write each partition appropriately to the target data placement.
      IntStream.range(0, dstParallelism).forEach(partitionIdx ->
        dataPlacement.put(
            runtimeEdge.getRuntimeEdgeId(), srcTask.getIndex(), partitionIdx, partitionedOutputList.get(partitionIdx)));
      break;
    case Range:
    default:
      throw new UnsupportedPartitionerException(
          new Exception(partitioner + " partitioning not yet supported"));
    }
  }
}
