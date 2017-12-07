/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.onyx.compiler.optimizer.pass.compiletime.annotating;

import edu.snu.onyx.common.dag.DAG;
import edu.snu.onyx.common.ir.edge.IREdge;
import edu.snu.onyx.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.onyx.common.ir.vertex.IRVertex;
import edu.snu.onyx.common.ir.vertex.MetricCollectionBarrierVertex;
import edu.snu.onyx.common.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.common.ir.edge.executionproperty.MetricCollectionProperty;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Pass to annotate the DAG for a job to perform data skew.
 * It specifies the outgoing Shuffle edges from MetricCollectionVertices with a MetricCollection ExecutionProperty
 * which lets the edge to know what metric collection it should perform.
 */
public final class DataSkewEdgeMetricCollectionPass extends AnnotatingPass {
  public DataSkewEdgeMetricCollectionPass() {
    super(ExecutionProperty.Key.MetricCollection, Stream.of(
        ExecutionProperty.Key.DataCommunicationPattern
    ).collect(Collectors.toSet()));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(v -> {
      // we only care about metric collection barrier vertices.
      if (v instanceof MetricCollectionBarrierVertex) {
        dag.getOutgoingEdgesOf(v).forEach(edge -> {
          // double checking.
          if (edge.getProperty(ExecutionProperty.Key.DataCommunicationPattern)
              .equals(DataCommunicationPatternProperty.Value.Shuffle)) {
            edge.setProperty(MetricCollectionProperty.of(MetricCollectionProperty.Value.DataSkewRuntimePass));
          }
        });
      }
    });
    return dag;
  }
}
