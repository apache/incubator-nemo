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

import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.edge.executionproperty.MetricCollectionProperty;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.MetricCollectTransform;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;

/**
 * Pass to annotate the IR DAG for skew handling.
 *
 * It specifies the target of dynamic optimization for skew handling
 * by setting appropriate {@link MetricCollectionProperty} to
 * outgoing shuffle edges from vertices with {@link MetricCollectTransform}.
 */
@Annotates(MetricCollectionProperty.class)
@Requires(CommunicationPatternProperty.class)
public final class SkewMetricCollectionPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public SkewMetricCollectionPass() {
    super(SkewMetricCollectionPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(v -> {
      // we only care about metric collection vertices.
      if (v instanceof OperatorVertex
        && ((OperatorVertex) v).getTransform() instanceof MetricCollectTransform) {
        dag.getOutgoingEdgesOf(v).forEach(edge -> {
          // double checking.
          if (edge.getPropertyValue(CommunicationPatternProperty.class).get()
              .equals(CommunicationPatternProperty.Value.Shuffle)) {
            edge.setPropertyPermanently(MetricCollectionProperty.of(
                MetricCollectionProperty.Value.DataSkewRuntimePass));
          }
        });
      }
    });
    return dag;
  }
}
