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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Runtime pass for Dynamic Task Sizing policy.
 */
public final class DynamicTaskSizingRuntimePass extends RunTimePass<Map<String, Long>> {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicTaskSizingRuntimePass.class.getName());
  private final String mapKey = "opt.parallelism";

  public DynamicTaskSizingRuntimePass() {
  }

  @Override
  public IRDAG apply(final IRDAG irdag, final Message<Map<String, Long>> mapMessage) {
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
    if (!representativeEdge.getDst().getPropertyValue(EnableDynamicTaskSizingProperty.class).get()) {
      return irdag;
    }
    final Map<String, Long> messageValue = mapMessage.getMessageValue();
    LOG.info("messageValue {}", messageValue);
    final int optimizedTaskSizeRatio = messageValue.get(mapKey).intValue();
    final int partitionerProperty = getPartitionerProperty(irdag);
    for (IREdge edge : edgesToOptimize) {
      if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
        .equals(CommunicationPatternProperty.Value.SHUFFLE)
        && !edge.getPropertyValue(PartitionerProperty.class).get().right().equals(partitionerProperty)) {
        throw new IllegalArgumentException();
      }
    }
    final int partitionUnit = partitionerProperty / optimizedTaskSizeRatio;
    edgesToOptimize.forEach(irEdge -> setSubPartitionProperty(irEdge, partitionUnit, partitionerProperty));
    edgesToOptimize.forEach(irEdge -> setDstVertexParallelismProperty(irEdge, partitionUnit, partitionerProperty));
    return irdag;
  }

  private int getPartitionerProperty(final IRDAG dag) {
    long jobSizeInBytes = dag.getInputSize();
    long jobSizeInGB = jobSizeInBytes / (1024 * 1024 * 1024);
    if (1 <= jobSizeInGB && jobSizeInGB < 10) {
      return 1024;
    } else if (10 <= jobSizeInGB && jobSizeInGB < 100) {
      return 2048;
    } else {
      return 4096;
    }
  }

  private void setSubPartitionProperty(final IREdge edge, final int growingFactor, final int partitionerProperty) {
    final int start = (int) edge.getPropertyValue(SubPartitionSetProperty.class).get().get(0).rangeBeginInclusive();
    final ArrayList<KeyRange> partitionSet = new ArrayList<>();
    int taskIndex = 0;
    for (int startIndex = start; startIndex < partitionerProperty; startIndex += growingFactor) {
      partitionSet.add(taskIndex, HashRange.of(startIndex, startIndex + growingFactor));
      taskIndex++;
    }
    edge.setPropertyPermanently(SubPartitionSetProperty.of(partitionSet));
  }

  private void setDstVertexParallelismProperty(final IREdge edge,
                                               final int partitionSize,
                                               final int partitionerProperty) {
    final int start = (int) edge.getPropertyValue(SubPartitionSetProperty.class).get().get(0).rangeBeginInclusive();
    final int newParallelism = (partitionerProperty - start) / partitionSize;
    edge.getDst().setPropertyPermanently(ParallelismProperty.of(newParallelism));
  }
}
