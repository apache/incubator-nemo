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
import edu.snu.nemo.common.ir.edge.executionproperty.MetricCollectionProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.edge.executionproperty.PartitionerProperty;
import edu.snu.nemo.common.ir.vertex.OperatorVertex;
import edu.snu.nemo.common.ir.vertex.transform.AggregateMetricTransform;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.Requires;

import java.util.List;

/**
 * Transient resource pass for tagging edges with {@link PartitionerProperty}.
 */
@Annotates(PartitionerProperty.class)
@Requires(MetricCollectionProperty.class)
public final class SkewPartitionerPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public SkewPartitionerPass() {
    super(SkewPartitionerPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.getVertices().forEach(v -> {
      if (v instanceof OperatorVertex
        && ((OperatorVertex) v).getTransform() instanceof AggregateMetricTransform) {
        final List<IREdge> outEdges = dag.getOutgoingEdgesOf(v);
        outEdges.forEach(edge -> {
          // double checking.
          if (MetricCollectionProperty.Value.DataSkewRuntimePass
            .equals(edge.getPropertyValue(MetricCollectionProperty.class).get())) {
            edge.setPropertyPermanently(PartitionerProperty.of(PartitionerProperty.Value.DataSkewHashPartitioner));
          }
        });
      }
    });
    return dag;
  }
}
