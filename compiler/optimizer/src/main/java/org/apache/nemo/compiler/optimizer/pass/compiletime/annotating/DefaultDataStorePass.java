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
import org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;

/**
 * Edge data store pass to process inter-stage memory store edges.
 */
@Annotates(DataStoreProperty.class)
public final class DefaultDataStorePass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public DefaultDataStorePass() {
    super(DefaultDataStorePass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(vertex -> {
      dag.getIncomingEdgesOf(vertex).stream()
          //.filter(edge -> !edge.getPropertyValue(DataStoreProperty.class).isPresent())
          .forEach(edge -> edge.setProperty(
              DataStoreProperty.of(DataStoreProperty.Value.Pipe)));
    });
    return dag;
  }
}
