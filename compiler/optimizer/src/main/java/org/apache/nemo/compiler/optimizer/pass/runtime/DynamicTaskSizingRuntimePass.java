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
package org.apache.nemo.compiler.optimizer.pass.runtime;

import org.apache.nemo.common.HashRange;
import org.apache.nemo.common.KeyRange;
import org.apache.nemo.common.exception.RuntimeOptimizationException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import org.apache.nemo.common.ir.edge.executionproperty.SubPartitionSetProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.EnableDynamicTaskSizingProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Runtime pass for Dynamic Task Sizing policy.
 */
public final class DynamicTaskSizingRuntimePass extends RunTimePass<Map<String, Long>> {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicTaskSizingRuntimePass.class.getName());
  private static final int PARTITIONER_PROPERTY_FOR_SMALL_JOB = 1024;
  private static final int PARTITIONER_PROPERTY_FOR_MEDIUM_JOB = 2048;
  private static final int PARTITIONER_PROPERTY_FOR_LARGE_JOB = 4096;
  private static final int LOWER_BOUND_SMALL_JOB_GB = 1;
  private static final int LOWER_BOUND_MEDIUM_JOB_GB = 10;
  private static final int LOWER_BOUND_LARGE_JOB_GB = 100;
  private final String mapKey = "opt.parallelism";

  /**
   * Default Constructor.
   */
  public DynamicTaskSizingRuntimePass() {
  }

  @Override
  public IRDAG apply(final IRDAG irdag,
                     final Message<Map<String, Long>> mapMessage) {
    final Set<IREdge> edgesToOptimize = mapMessage.getExaminedEdges();
    final Set<IRVertex> stageVertices = edgesToOptimize.stream().map(IREdge::getDst).collect(Collectors.toSet());
    irdag.topologicalDo(v -> {
      if (stageVertices.contains(v)) {
        edgesToOptimize.addAll(irdag.getIncomingEdgesOf(v));
      }
    });
    LOG.info("Examined edges {}", edgesToOptimize.stream().map(IREdge::getId).collect(Collectors.toList()));

    final IREdge representativeEdge = edgesToOptimize.iterator().next();
    // double check
    if (!representativeEdge.getDst().getPropertyValue(EnableDynamicTaskSizingProperty.class).orElse(false)) {
      return irdag;
    }
    final Map<String, Long> messageValue = mapMessage.getMessageValue();
    LOG.info("messageValue {}", messageValue);
    final int optimizedTaskSizeRatio = messageValue.get(mapKey).intValue();
    final int partitionerProperty = getPartitionerProperty(irdag);
    for (IREdge edge : edgesToOptimize) {
      if (CommunicationPatternProperty.Value.SHUFFLE.equals(edge.getPropertyValue(CommunicationPatternProperty.class)
        .get()) && partitionerProperty != (edge.getPropertyValue(PartitionerProperty.class).get().right())) {
        throw new IllegalArgumentException();
      }
    }
    final int partitionUnit = partitionerProperty / optimizedTaskSizeRatio;
    edgesToOptimize.forEach(irEdge -> setSubPartitionSetProperty(irEdge, partitionUnit, partitionerProperty));
    edgesToOptimize.forEach(irEdge -> setDstVertexParallelismProperty(irEdge, partitionUnit, partitionerProperty));
    return irdag;
  }

  /**
   * Get PartitionerProperty by job size of the given dag.
   * Please note that this runtime optimization is not intended to operate on jobs with size smaller than
   * LOWER_BOUND_SMALL_JOB_GB.
   * @param dag   dag to observe.
   * @return      partitioner property.
   */
  private int getPartitionerProperty(final IRDAG dag) {
    long sourceInputDataSizeInBytes = dag.getInputSize();
    long sourceInputDataSizeInGB = sourceInputDataSizeInBytes / (1024 * 1024 * 1024);
    if (sourceInputDataSizeInGB < LOWER_BOUND_SMALL_JOB_GB) {
      final String message = String.format("Job size must be greater than %d GB to run DynamicTaskSizingRuntimePass",
        LOWER_BOUND_SMALL_JOB_GB);
      throw new RuntimeOptimizationException(message);
    } else if (sourceInputDataSizeInGB < LOWER_BOUND_MEDIUM_JOB_GB) {
      return PARTITIONER_PROPERTY_FOR_SMALL_JOB;
    } else if (sourceInputDataSizeInGB < LOWER_BOUND_LARGE_JOB_GB) {
      return PARTITIONER_PROPERTY_FOR_MEDIUM_JOB;
    } else {
      return PARTITIONER_PROPERTY_FOR_LARGE_JOB;
    }
  }

  /**
   * Update SubPartitionSetProperty of the given edge using partitionerProperty and growingFactor.
   * Since this function only re-splits the allocated partitions of the edge, but not changes its range, the start and
   * end value of the before and after partitions should be equal.
   * The length of updated SubPartitionSetProperty indicates the optimized parallelism of the destination of the edge.
   *
   * Note:
   * Since the PartitionerProperty is dependent on Job size and SubPartitionSetProperty depends on PartitionerProperty,
   * The RangeEndExclusive() value of the last element in SubPartitionerProperty of the edge must be equal with the
   * PartitionerProperty of the edge.
   * [Example case]
   * Original SubPartitionSetProperty: [[512, 4096)]
   * growing factor: 64
   * partitioner property: 4096
   *
   * start value in code: 512
   *
   * Updated partitioner property: [[512, 576), [576, 640), [640, 704), ... , [3968, 4032), [4032, 4096)]
   * In this case, the updated parallelism property of the destination vertex of the edge is 56.
   * Updated parallelism property: (4096 - 512) / 64.
   *
   * @param edge                Edge to update SubPartitionSetProperty.
   * @param growingFactor       The length of the updated KeyRange.
   *                            That is, keyRange.rangeEndExclusive() - keyRange.rangeBeginInclusive().
   *                            The length of the range of re-splitted partitions are all equal for now.
   * @param partitionerProperty The total size of partitions in integer.
   */
  private void setSubPartitionSetProperty(final IREdge edge, final int growingFactor, final int partitionerProperty) {
    final List<KeyRange> keyRanges = edge.getPropertyValue(SubPartitionSetProperty.class)
      .orElseThrow(() -> new RuntimeOptimizationException("SubPartitionSet Property of edge is missing."));
    if (keyRanges.isEmpty()) {
      return;
    }
    final int start = (int) keyRanges.get(0).rangeBeginInclusive();
    final ArrayList<KeyRange> partitionSet = new ArrayList<>();
    int taskIndex = 0;
    for (int startIndex = start; startIndex < partitionerProperty; startIndex += growingFactor) {
      partitionSet.add(taskIndex, HashRange.of(startIndex, startIndex + growingFactor));
      taskIndex++;
    }
    edge.setPropertyPermanently(SubPartitionSetProperty.of(partitionSet));
  }

  /**
   * Update the ParallelismProperty of the destination vertex of the given edge.
   * For more information, please refer to the upper setSubPartitionSetProperty() method.
   * @param edge                  Incoming edge of the vertex to optimize.
   * @param partitionSize         The length of the KeyRange.
   * @param partitionerProperty   The total size of partitions in integer.
   */
  private void setDstVertexParallelismProperty(final IREdge edge,
                                               final int partitionSize,
                                               final int partitionerProperty) {
    final List<KeyRange> keyRanges = edge.getPropertyValue(SubPartitionSetProperty.class)
      .orElseThrow(() -> new RuntimeOptimizationException("SubpartitionSet Property of the edge is missing."));
    final int start = (int) keyRanges.get(0).rangeBeginInclusive();
    final int newParallelism = (partitionerProperty - start) / partitionSize;
    edge.getDst().setPropertyPermanently(ParallelismProperty.of(newParallelism));
  }
}
