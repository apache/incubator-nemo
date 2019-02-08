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
package org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping;

import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.utility.MessageBarrierVertex;
import org.apache.nemo.common.ir.vertex.utility.SamplingVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Optimizes the PartitionSet property of shuffle edges to handle data skews using the SamplingVertex.
 *
 * This pass effectively partitions the IRDAG by non-oneToOne edges, clones each subDAG partition using SamplingVertex
 * to process sampled data, and executes each cloned partition prior to executing the corresponding original partition.
 *
 * Suppose the IRDAG is partitioned into two sub-DAGs as follows:
 * P1 -> P2
 *
 * Then, this pass will produce something like:
 * P1' -> P1 -> P2
 *           -> P2' -> P2
 * where Px' consists of SamplingVertex objects that clone the execution of Px.
 *
 * For each Px' this pass also inserts a MessageBarrierVertex, to use its data statistics for dynamically optimizing
 * the execution behaviors of Px.
 */
@Requires(CommunicationPatternProperty.class)
public final class SamplingSkewReshapingPass extends ReshapingPass {
  private static final Logger LOG = LoggerFactory.getLogger(SamplingSkewReshapingPass.class.getName());
  private static final float SAMPLE_RATE = 0.1f;

  /**
   * Default constructor.
   */
  public SamplingSkewReshapingPass() {
    super(SamplingSkewReshapingPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    dag.topologicalDo(v -> {
      for (final IREdge e : dag.getIncomingEdgesOf(v)) {
        if (!CommunicationPatternProperty.Value.OneToOne.equals(
          e.getPropertyValue(CommunicationPatternProperty.class).get())) {
          // Compute the partition and its source vertices
          final IRVertex shuffleWriter = e.getSrc();
          final Set<IRVertex> partitionAll = recursivelyBuildPartition(shuffleWriter, dag);
          final Set<IRVertex> partitionSources = partitionAll.stream().filter(vertexInPartition ->
            dag.getIncomingEdgesOf(vertexInPartition).stream()
              .map(Edge::getSrc)
              .allMatch(partitionAll::contains)
          ).collect(Collectors.toSet());

          // Insert sampling vertices.
          final Set<SamplingVertex> samplingVertices = partitionAll
            .stream()
            .map(vertexInPartition -> new SamplingVertex(vertexInPartition, SAMPLE_RATE))
            .collect(Collectors.toSet());
          dag.insert(samplingVertices, partitionSources);

          // Insert the message vertex.
          // TODO: reuse code from the existing skew pass
          final MessageBarrierVertex mbv =
          dag.insert();
        }
      }
    });

    return dag;
  }

  private Set<IRVertex> recursivelyBuildPartition(final IRVertex curVertex, final IRDAG dag) {
    final Set<IRVertex> unionSet = new HashSet<>();
    unionSet.add(curVertex);
    for (final IREdge inEdge : dag.getIncomingEdgesOf(curVertex)) {
      if (CommunicationPatternProperty.Value.OneToOne
        .equals(inEdge.getPropertyValue(CommunicationPatternProperty.class).get())
        && DataStoreProperty.Value.MemoryStore
        .equals(inEdge.getPropertyValue(DataStoreProperty.class).get())
        && dag.getIncomingEdgesOf(curVertex).size() == 1) {
        unionSet.addAll(recursivelyBuildPartition(inEdge.getSrc(), dag));
      }
    }
    return unionSet;
  }
}
