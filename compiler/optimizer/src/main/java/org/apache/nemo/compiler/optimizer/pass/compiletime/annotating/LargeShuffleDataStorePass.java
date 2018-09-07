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
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

/**
 * A pass to optimize large shuffle by tagging edges.
 * This pass handles the DataStore ExecutionProperty.
 */
@Annotates(DataStoreProperty.class)
@Requires(CommunicationPatternProperty.class)
public final class LargeShuffleDataStorePass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public LargeShuffleDataStorePass() {
    super(LargeShuffleDataStorePass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      // Find the merger vertex inserted by reshaping pass.
      if (dag.getIncomingEdgesOf(vertex).stream().anyMatch(irEdge ->
              CommunicationPatternProperty.Value.Shuffle
          .equals(irEdge.getPropertyValue(CommunicationPatternProperty.class).get()))) {
        dag.getIncomingEdgesOf(vertex).forEach(edgeToMerger -> {
          if (CommunicationPatternProperty.Value.Shuffle
          .equals(edgeToMerger.getPropertyValue(CommunicationPatternProperty.class).get())) {
            // Pass data through memory to the merger vertex.
            edgeToMerger.setPropertyPermanently(DataStoreProperty.of(DataStoreProperty.Value.SerializedMemoryStore));
          }
        });
        dag.getOutgoingEdgesOf(vertex).forEach(edgeFromMerger ->
            // Merge the input data and write it immediately to the remote disk.
            edgeFromMerger.setPropertyPermanently(DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore)));
      }
    });
    return dag;
  }
}
