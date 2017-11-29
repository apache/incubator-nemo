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
package edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.common.ir.edge.executionproperty.PartitionerProperty;

import java.util.Collections;
import java.util.List;

/**
 * Optimization pass for tagging {@link PartitionerProperty}
 * execution property.
 */
public final class DefaultPartitionerPass extends AnnotatingPass {
  public static final String SIMPLE_NAME = "DefaultPartitionerPass";

  public DefaultPartitionerPass() {
    super(ExecutionProperty.Key.Partitioner, Collections.singleton(ExecutionProperty.Key.DataCommunicationPattern));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      inEdges.forEach(edge -> {
        if (edge.getProperty(ExecutionProperty.Key.Partitioner) == null) {
          if (edge.getProperty(ExecutionProperty.Key.DataCommunicationPattern)
              .equals(DataCommunicationPatternProperty.Value.ScatterGather)) {
            edge.setProperty(PartitionerProperty.of(PartitionerProperty.Value.HashPartitioner));
          } else {
            edge.setProperty(PartitionerProperty.of(PartitionerProperty.Value.IntactPartitioner));
          }
        }
      });
    });
    return dag;
  }
}
