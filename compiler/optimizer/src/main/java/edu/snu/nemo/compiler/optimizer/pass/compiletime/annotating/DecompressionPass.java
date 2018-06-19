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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.CompressionProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DecompressionProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.StageIdProperty;


/**
 * A pass for applying decompression algorithm for data flowing between vertices.
 * It always
 */
public final class DecompressionPass extends AnnotatingPass {

  /**
   * Constructor.
   */
  public DecompressionPass() {
    super(CompressionProperty.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(vertex -> dag.getIncomingEdgesOf(vertex).stream()
        .filter(e -> !vertex.getPropertyValue(StageIdProperty.class).get()
            .equals(e.getSrc().getPropertyValue(StageIdProperty.class).get()))
        // Find edges which have a compression property but not decompression property.
        .filter(edge -> edge.getPropertyValue(CompressionProperty.class).isPresent()
            && !edge.getPropertyValue(DecompressionProperty.class).isPresent())
        .forEach(edge -> edge.setProperty(DecompressionProperty.of(
            edge.getPropertyValue(CompressionProperty.class).get()))));

    return dag;
  }
}
