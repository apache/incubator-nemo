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
package org.apache.nemo.runtime.executor.datatransfer;

import org.apache.nemo.common.KeyExtractor;
import org.apache.nemo.common.exception.*;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.executor.data.partitioner.*;

import java.util.*;

/**
 * Represents the output data transfer from a task.
 */
abstract class OutputWriter extends DataTransfer implements AutoCloseable {
  final RuntimeEdge<?> runtimeEdge;
  final IRVertex dstIrVertex;
  long writtenBytes;
  Partitioner partitioner;

  /**
   * Constructor.
   *
   * @param hashRangeMultiplier the {@link org.apache.nemo.conf.JobConf.HashRangeMultiplier}.
   * @param dstIrVertex         the destination IR vertex.
   * @param runtimeEdge         the {@link RuntimeEdge}.
   */
  OutputWriter(final int hashRangeMultiplier,
               final IRVertex dstIrVertex,
               final RuntimeEdge<?> runtimeEdge) {
    super(runtimeEdge.getId());
    this.runtimeEdge = runtimeEdge;
    this.dstIrVertex = dstIrVertex;

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
  }

  /**
   * Writes output element depending on the communication pattern of the edge.
   *
   * @param element the element to write.
   */
  public abstract void write(final Object element);

  /**
   * Notifies that all writes are done.
   */
  public abstract void close();

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
  protected int getExpectedRead() {
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
