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
import edu.snu.onyx.compiler.ir.IREdge;
import edu.snu.onyx.compiler.ir.IRVertex;
import edu.snu.onyx.compiler.ir.MetricCollectionBarrierVertex;
import edu.snu.onyx.compiler.ir.executionproperty.ExecutionProperty;
import edu.snu.onyx.compiler.ir.executionproperty.edge.MetricCollectionProperty;
import edu.snu.onyx.compiler.optimizer.pass.runtime.DataSkewRuntimePass;
import edu.snu.onyx.runtime.executor.datatransfer.communication.ScatterGather;

/**
 * Pass to annotate the DAG for a job to perform data skew.
 * It specifies the outgoing ScatterGather edges from MetricCollectionVertices with a MetricCollection ExecutionProperty
 * which lets the edge to know what metric collection it should perform.
 */
public final class DataSkewEdgeMetricCollectionPass extends AnnotatingPass {
  public static final String SIMPLE_NAME = "DataSkewEdgeMetricCollectionPass";

  public DataSkewEdgeMetricCollectionPass() {
    super(ExecutionProperty.Key.MetricCollection);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(v -> {
      // we only care about metric collection barrier vertices.
      if (v instanceof MetricCollectionBarrierVertex) {
        dag.getOutgoingEdgesOf(v).forEach(edge -> {
          // double checking.
          if (ScatterGather.class.equals(edge.getProperty(ExecutionProperty.Key.DataCommunicationPattern))) {
            edge.setProperty(MetricCollectionProperty.of(DataSkewRuntimePass.class));
          }
        });
      }
    });
    return dag;
  }
}
