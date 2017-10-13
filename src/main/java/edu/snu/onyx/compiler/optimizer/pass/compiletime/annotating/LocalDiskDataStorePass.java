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
import edu.snu.onyx.compiler.ir.IREdge;
import edu.snu.onyx.compiler.ir.IRVertex;
import edu.snu.onyx.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.compiler.ir.executionproperty.edge.DataStoreProperty;
import edu.snu.onyx.runtime.executor.data.LocalFileStore;
import edu.snu.onyx.runtime.executor.data.MemoryStore;
import edu.snu.onyx.runtime.executor.datatransfer.communication.ScatterGather;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Pass for tagging shuffle edges with "Local Disk" DataStore ExecutionProperty.
 */
public final class LocalDiskDataStorePass extends AnnotatingPass {
  public static final String SIMPLE_NAME = "LocalDiskDataStorePass";

  public LocalDiskDataStorePass() {
    super(ExecutionProperty.Key.DataFlowModel, Stream.of(
        ExecutionProperty.Key.ExecutorPlacement
    ).collect(Collectors.toSet()));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      if (!inEdges.isEmpty()) {
        inEdges.forEach(edge -> {
          if (ScatterGather.class.equals(edge.getProperty(ExecutionProperty.Key.DataCommunicationPattern))) {
            edge.setProperty(DataStoreProperty.of(LocalFileStore.class));
          } else {
            edge.setProperty(DataStoreProperty.of(MemoryStore.class));
          }
        });
      }
    });
    return dag;
  }
}
