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
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.MetricCollectionBarrierVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.DynamicOptimizationProperty;
import edu.snu.nemo.common.ir.vertex.executionproperty.ResourceSkewedDataProperty;

import java.util.List;

/**
 * Pass to annotate the DAG for a job to perform data skew.
 * It specifies which optimization to perform on the MetricCollectionBarrierVertex.
 */
public final class SkewResourceSkewedDataPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public SkewResourceSkewedDataPass() {
    super(DynamicOptimizationProperty.class);
  }

  private boolean hasMetricCollectionBarrierVertexAsParent(final DAG<IRVertex, IREdge> dag,
                                                           final IRVertex v) {
    List<IRVertex> parents = dag.getParents(v.getId());
    for (IRVertex parent : parents) {
      if (parent instanceof MetricCollectionBarrierVertex) {
        return true;
      }
    }
    return false;
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().stream()
        .filter(v -> v instanceof MetricCollectionBarrierVertex)
        .forEach(v -> v.setProperty(DynamicOptimizationProperty
            .of(DynamicOptimizationProperty.Value.DataSkewRuntimePass)));
    dag.getVertices().stream()
        .filter(v -> hasMetricCollectionBarrierVertexAsParent(dag, v)
            && !v.getExecutionProperties().containsKey(ResourceSkewedDataProperty.class))
        .forEach(v -> v.getExecutionProperties().put(ResourceSkewedDataProperty.of(true)));

    return dag;
  }
}
