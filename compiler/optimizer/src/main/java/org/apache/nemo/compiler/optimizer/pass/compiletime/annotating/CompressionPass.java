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
package org.apache.nemo.compiler.optimizer.pass.compiletime.annotating;

import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.executionproperty.CompressionProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DecompressionProperty;


/**
 * A pass for applying compression algorithm for data flowing between vertices.
 */
@Annotates(CompressionProperty.class)
public final class CompressionPass extends AnnotatingPass {
  private final CompressionProperty.Value compression;

  /**
   * Default constructor. Uses LZ4 as default.
   */
  public CompressionPass() {
    this(CompressionProperty.Value.LZ4);
  }

  /**
   * Constructor.
   * @param compression Compression to apply on edges.
   */
  public CompressionPass(final CompressionProperty.Value compression) {
    super(CompressionPass.class);
    this.compression = compression;
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    dag.topologicalDo(vertex -> dag.getIncomingEdgesOf(vertex).forEach(edge -> {
      if (!edge.getPropertyValue(CompressionProperty.class).isPresent()
        && !edge.getPropertyValue(DecompressionProperty.class).isPresent()) {
        edge.setProperty(CompressionProperty.of(compression));
        edge.setProperty(DecompressionProperty.of(compression));
      }
    }));
    return dag;
  }
}
