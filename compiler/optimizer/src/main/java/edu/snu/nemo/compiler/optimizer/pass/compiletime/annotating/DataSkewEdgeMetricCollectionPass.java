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
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.MetricCollectionBarrierVertex;
import edu.snu.nemo.common.ir.edge.executionproperty.MetricCollectionProperty;

import java.util.Collections;

/**
 * Pass to annotate the DAG for a job to perform data skew.
 * It specifies the outgoing Shuffle edges from MetricCollectionVertices with a MetricCollection ExecutionProperty
 * which lets the edge to know what metric collection it should perform.
 */
public final class DataSkewEdgeMetricCollectionPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public DataSkewEdgeMetricCollectionPass() {
    super(MetricCollectionProperty.class, Collections.singleton(DataCommunicationPatternProperty.class));
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(v -> {
      // we only care about metric collection barrier vertices.
      if (v instanceof MetricCollectionBarrierVertex) {
        dag.getOutgoingEdgesOf(v).forEach(edge -> {
          // double checking.
          if (edge.getPropertyValue(DataCommunicationPatternProperty.class).get()
              .equals(DataCommunicationPatternProperty.Value.Shuffle)) {
            edge.setProperty(MetricCollectionProperty.of(MetricCollectionProperty.Value.DataSkewRuntimePass));
          }
        });
      }
    });
    return dag;
  }
}
