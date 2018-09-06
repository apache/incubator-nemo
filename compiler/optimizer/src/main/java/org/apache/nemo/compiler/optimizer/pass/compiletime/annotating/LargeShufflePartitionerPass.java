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
package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.List;

/**
 * A pass to support Sailfish-like shuffle by tagging edges.
 * This pass modifies the partitioner property from {@link org.apache.nemo.common.ir.vertex.transform.RelayTransform}
 * to write an element as a partition.
 * This enables that every byte[] element, which was a partition for the reduce task, becomes one partition again
 * and flushed to disk write after it is relayed.
 */
@Annotates(PartitionerProperty.class)
@Requires(CommunicationPatternProperty.class)
public final class LargeShufflePartitionerPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public LargeShufflePartitionerPass() {
    super(LargeShufflePartitionerPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      inEdges.forEach(edge -> {
        if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
            .equals(CommunicationPatternProperty.Value.Shuffle)) {
          dag.getOutgoingEdgesOf(edge.getDst())
              .forEach(edgeFromRelay ->
                  edgeFromRelay.setPropertyPermanently(PartitionerProperty.of(
                      PartitionerProperty.Value.DedicatedKeyPerElementPartitioner)));
        }
      });
    });
    return dag;
  }
}
