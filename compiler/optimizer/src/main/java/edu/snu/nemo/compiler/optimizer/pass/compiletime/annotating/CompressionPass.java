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
import edu.snu.nemo.common.ir.vertex.IRVertex;


/**
 * A pass for applying compression algorithm for data flowing between vertices.
 */
public final class CompressionPass extends AnnotatingPass {
  private final CompressionProperty.Value compression;

  /**
   * Default constructor. Uses LZ4 as default.
   */
  public CompressionPass() {
    super(CompressionProperty.class);
    this.compression = CompressionProperty.Value.LZ4;
  }

  /**
   * Constructor.
   * @param compression Compression to apply on edges.
   */
  public CompressionPass(final CompressionProperty.Value compression) {
    super(CompressionProperty.class);
    this.compression = compression;
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(vertex -> dag.getIncomingEdgesOf(vertex).stream()
        .filter(edge -> !edge.getPropertyValue(CompressionProperty.class).isPresent())
        .forEach(edge -> edge.setProperty(CompressionProperty.of(compression))));

    return dag;
  }
}
