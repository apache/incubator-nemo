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

import org.apache.nemo.common.coder.BytesEncoderFactory;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.List;

/**
 * A pass to optimize large shuffle by tagging edges.
 * This pass modifies the encoder property toward {@link org.apache.nemo.common.ir.vertex.transform.RelayTransform}
 * to write data as byte arrays.
 */
@Annotates(EncoderProperty.class)
@Requires(CommunicationPatternProperty.class)
public final class LargeShuffleEncoderPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public LargeShuffleEncoderPass() {
    super(LargeShuffleEncoderPass.class);
  }

  @Override
  public void optimize(final IRDAG dag) {
    dag.getVertices().forEach(vertex -> {
      final List<IREdge> inEdges = dag.getIncomingEdgesOf(vertex);
      inEdges.forEach(edge -> {
        if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
            .equals(CommunicationPatternProperty.Value.Shuffle)) {
          dag.getOutgoingEdgesOf(edge.getDst())
              .forEach(edgeFromRelay ->
                  edgeFromRelay.setPropertyPermanently(EncoderProperty.of(BytesEncoderFactory.of())));
        }
      });
    });
  }
}
