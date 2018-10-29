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

import org.apache.nemo.common.KeyExtractor;
import org.apache.nemo.common.exception.UnsupportedPartitionerException;
import org.apache.nemo.common.ir.edge.executionproperty.KeyExtractorProperty;
import org.apache.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.runtime.common.plan.RuntimeEdge;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.executor.data.partitioner.*;

import java.util.*;

/**
 * Represents the output data transfer from a task.
 */
public interface OutputWriter {
  /**
   * Writes output element depending on the communication pattern of the edge.
   *
   * @param element the element to write.
   */
  void write(final Object element);

  /**
   * @return the total written bytes.
   */
  Optional<Long> getWrittenBytes();

  void close();


  static Partitioner getPartitioner(final RuntimeEdge runtimeEdge,
                                    final int hashRangeMultiplier) {
    final StageEdge stageEdge = (StageEdge) runtimeEdge;
    final PartitionerProperty.Value partitionerPropertyValue =
      (PartitionerProperty.Value) runtimeEdge.getPropertyValueOrRuntimeException(PartitionerProperty.class);
    final int dstParallelism =
      stageEdge.getDstIRVertex().getPropertyValue(ParallelismProperty.class).get();

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
        partitioner = new DataSkewHashPartitioner(hashRangeMultiplier, dstParallelism, dataSkewKeyExtractor);
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
