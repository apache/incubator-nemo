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
package org.apache.nemo.runtime.common.partitioner;

import org.apache.nemo.common.KeyExtractor;
import org.apache.nemo.common.exception.UnsupportedPartitionerException;
import org.apache.nemo.common.ir.edge.executionproperty.KeyExtractorProperty;
import org.apache.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.StageEdge;

import java.io.Serializable;

/**
 * This interface represents the way of partitioning output data from a source task.
 * It takes an element and designates key of partition to write the element,
 * according to the number of destination tasks, the key of each element, etc.
 *
 * @param <K> the key type of the partition to write.
 */
public interface Partitioner<K extends Serializable> {

  /**
   * Divides the output data from a task into multiple blocks.
   *
   * @param element the output element from a source task.
   * @return the key of the partition in the block to write the element.
   */
  K partition(Object element);

  /**
   * Gets appropriate partitioner for an edge.
   *
   * @param runtimeEdge the runtime edge.
   * @return the partitioner for the edge.
   */
  static Partitioner getPartitioner(final RuntimeEdge runtimeEdge) {
    final StageEdge stageEdge = (StageEdge) runtimeEdge;
    final PartitionerProperty.Value partitionerPropertyValue =
      (PartitionerProperty.Value) runtimeEdge.getPropertyValueOrRuntimeException(PartitionerProperty.class);
    final int dstParallelism =
      stageEdge.getDstIRVertex().getPropertyValue(ParallelismProperty.class)
        .orElseThrow(() -> new RuntimeException("No parallelism in edge " + runtimeEdge.getId()));

    final Partitioner partitioner;
    switch (partitionerPropertyValue) {
      case IntactPartitioner:
        partitioner = new IntactPartitioner();
        break;
      case HashPartitioner:
        final KeyExtractor hashKeyExtractor =
          (KeyExtractor) runtimeEdge.getPropertyValueOrRuntimeException(KeyExtractorProperty.class);
        partitioner = new HashPartitioner(dstParallelism, hashKeyExtractor);
        break;
      case DataSkewHashPartitioner:
        final KeyExtractor dataSkewKeyExtractor =
          (KeyExtractor) runtimeEdge.getPropertyValueOrRuntimeException(KeyExtractorProperty.class);
        partitioner = new DataSkewHashPartitioner(dstParallelism, dataSkewKeyExtractor);
        break;
      case DedicatedKeyPerElementPartitioner:
        partitioner = new DedicatedKeyPerElementPartitioner();
        break;
      default:
        throw new UnsupportedPartitionerException(
          new Throwable("Partitioner " + partitionerPropertyValue + " is not supported."));
    }
    return partitioner;
  }
}
