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
package org.apache.nemo.common.partitioner;

import org.apache.nemo.common.KeyExtractor;
import org.apache.nemo.common.exception.UnsupportedPartitionerException;
import org.apache.nemo.common.ir.edge.executionproperty.KeyExtractorProperty;
import org.apache.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import org.apache.nemo.common.ir.executionproperty.EdgeExecutionProperty;
import org.apache.nemo.common.ir.executionproperty.ExecutionPropertyMap;
import org.apache.nemo.common.ir.executionproperty.VertexExecutionProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;

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
   * @param edgeProperties edge properties.
   * @param dstProperties  vertex properties.
   * @return the partitioner.
   */
  static Partitioner getPartitioner(final ExecutionPropertyMap<EdgeExecutionProperty> edgeProperties,
                                    final ExecutionPropertyMap<VertexExecutionProperty> dstProperties) {
    final PartitionerProperty.Type type =
      edgeProperties.get(PartitionerProperty.class).get().left();
    final Partitioner partitioner;
    switch (type) {
      case INTACT:
        partitioner = new IntactPartitioner();
        break;
      case DEDICATED_KEY_PER_ELEMENT:
        partitioner = new DedicatedKeyPerElementPartitioner();
        break;
      case HASH:
        final int numOfPartitions = edgeProperties.get(PartitionerProperty.class).get().right();
        final int actualNumOfPartitions = (numOfPartitions == PartitionerProperty.NUM_EQUAL_TO_DST_PARALLELISM)
          ? dstProperties.get(ParallelismProperty.class).get()
          : numOfPartitions;
        final KeyExtractor keyExtractor = edgeProperties.get(KeyExtractorProperty.class).get();
        partitioner = new HashPartitioner(actualNumOfPartitions, keyExtractor);
        break;
      default:
        throw new UnsupportedPartitionerException(
          new Throwable("Partitioner " + type.toString() + " is not supported."));
    }
    return partitioner;
  }
}
