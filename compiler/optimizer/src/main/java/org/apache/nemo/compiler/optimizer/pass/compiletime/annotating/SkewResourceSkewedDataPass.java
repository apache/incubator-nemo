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
import org.apache.nemo.common.ir.edge.executionproperty.MessageIdProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceSkewedDataProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

/**
 * Pass to annotate the IR DAG for skew handling.
 *
 * It marks children and descendents of vertex with {@link ResourceSkewedDataProperty},
 * which collects task-level statistics used for dynamic optimization,
 * with {@link ResourceSkewedDataProperty} to perform skewness-aware scheduling.
 */
@Annotates(ResourceSkewedDataProperty.class)
@Requires(MessageIdProperty.class)
public final class SkewResourceSkewedDataPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public SkewResourceSkewedDataPass() {
    super(SkewResourceSkewedDataPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    dag.getVertices()
      .forEach(v -> dag.getOutgoingEdgesOf(v).stream()
        .filter(edge -> edge.getPropertyValue(MessageIdProperty.class).isPresent())
        .forEach(skewEdge -> {
          final IRVertex dstV = skewEdge.getDst();
          dstV.setProperty(ResourceSkewedDataProperty.of(true));
          dag.getDescendants(dstV.getId()).forEach(descendentV -> {
            descendentV.getExecutionProperties().put(ResourceSkewedDataProperty.of(true));
          });
        })
      );
    return dag;
  }
}
