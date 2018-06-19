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

import edu.snu.nemo.common.coder.DecoderFactory;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DecoderProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;

/**
 * Pass for initiating IREdge Decoder ExecutionProperty with default dummy coder.
 */
public final class DefaultEdgeDecoderPass extends AnnotatingPass {

  private static final DecoderProperty DEFAULT_DECODER_PROPERTY =
      DecoderProperty.of(DecoderFactory.DUMMY_DECODER_FACTORY);

  /**
   * Default constructor.
   */
  public DefaultEdgeDecoderPass() {
    super(DecoderProperty.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(irVertex ->
        dag.getIncomingEdgesOf(irVertex).forEach(irEdge -> {
          if (!irEdge.getPropertyValue(DecoderProperty.class).isPresent()) {
            irEdge.setProperty(DEFAULT_DECODER_PROPERTY);
          }
        }));
    return dag;
  }
}
