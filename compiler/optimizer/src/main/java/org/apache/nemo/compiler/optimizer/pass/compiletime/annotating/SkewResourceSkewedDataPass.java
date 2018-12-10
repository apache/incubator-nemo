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
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.DynamicOptimizationProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ResourceSkewedDataProperty;
import org.apache.nemo.common.ir.vertex.transform.MetricCollectTransform;

import java.util.List;

/**
 * Pass to annotate the IR DAG for skew handling.
 *
 * It marks children and descendents of vertex with {@link MetricCollectTransform},
 * which collects task-level statistics used for dynamic optimization,
 * with {@link ResourceSkewedDataProperty} to perform skewness-aware scheduling.
 */
@Annotates(DynamicOptimizationProperty.class)
public final class SkewResourceSkewedDataPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public SkewResourceSkewedDataPass() {
    super(SkewResourceSkewedDataPass.class);
  }

  /**
   * @param dag that contains the {@code v}.
   * @param v to inspect.
   * @return whether or not the vertex has parent with MetricCollectTransform.
   */
  private boolean hasParentWithMetricCollectTransform(final DAG<IRVertex, IREdge> dag,
                                                      final IRVertex v) {
    List<IRVertex> parents = dag.getParents(v.getId());
    for (IRVertex parent : parents) {
      if (parent instanceof OperatorVertex
        && ((OperatorVertex) v).getTransform() instanceof MetricCollectTransform) {
        return true;
      }
    }
    return false;
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().stream()
        .filter(v -> v instanceof OperatorVertex
          && ((OperatorVertex) v).getTransform() instanceof MetricCollectTransform)
      .forEach(v -> v.setProperty(DynamicOptimizationProperty
            .of(DynamicOptimizationProperty.Value.DataSkewRuntimePass)));

    dag.getVertices().stream()
        .filter(v -> hasParentWithMetricCollectTransform(dag, v)
            && !v.getExecutionProperties().containsKey(ResourceSkewedDataProperty.class))
        .forEach(childV -> {
          childV.getExecutionProperties().put(ResourceSkewedDataProperty.of(true));
          dag.getDescendants(childV.getId()).forEach(descendentV -> {
            descendentV.getExecutionProperties().put(ResourceSkewedDataProperty.of(true));
          });
        });

    return dag;
  }
}
