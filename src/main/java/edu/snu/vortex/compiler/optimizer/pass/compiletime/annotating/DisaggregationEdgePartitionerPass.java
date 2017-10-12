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
package edu.snu.vortex.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.vortex.common.dag.DAG;
import edu.snu.vortex.compiler.ir.IREdge;
import edu.snu.vortex.compiler.ir.IRVertex;
import edu.snu.vortex.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.vortex.compiler.ir.executionproperty.edge.PartitionerProperty;
import edu.snu.vortex.runtime.executor.data.GlusterFileStore;
import edu.snu.vortex.runtime.executor.datatransfer.communication.ScatterGather;
import edu.snu.vortex.runtime.executor.datatransfer.partitioning.IFileHashPartitioner;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Pass which enables I-File style write optimization.
 * It sets IFileWrite execution property on ScatterGather edges with RemoteFile partition store.
 */
public final class DisaggregationEdgePartitionerPass extends AnnotatingPass {
  public static final String SIMPLE_NAME = "DisaggregationEdgePartitionerPass";

  public DisaggregationEdgePartitionerPass() {
    super(ExecutionProperty.Key.WriteOptimization, Stream.of(
        ExecutionProperty.Key.DataStore,
        ExecutionProperty.Key.DataCommunicationPattern
    ).collect(Collectors.toSet()));
  }

  @Override
  public String getName() {
    return SIMPLE_NAME;
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      inEdges.forEach(edge -> {
        if (ScatterGather.class.equals(edge.getProperty(ExecutionProperty.Key.DataCommunicationPattern))
            && GlusterFileStore.class.equals(edge.getProperty(ExecutionProperty.Key.DataStore))) {
          edge.setProperty(PartitionerProperty.of(IFileHashPartitioner.class));
        }
      });
    });
    return dag;
  }
}
