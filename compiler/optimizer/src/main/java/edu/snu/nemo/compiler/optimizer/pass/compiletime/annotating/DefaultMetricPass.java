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

import edu.snu.nemo.common.DataSkewMetricFactory;
import edu.snu.nemo.common.HashRange;
import edu.snu.nemo.common.KeyRange;
import edu.snu.nemo.common.dag.DAG;
import edu.snu.nemo.common.ir.edge.IREdge;
import edu.snu.nemo.common.ir.edge.executionproperty.DataCommunicationPatternProperty;
import edu.snu.nemo.common.ir.edge.executionproperty.DataSkewMetricProperty;
import edu.snu.nemo.common.ir.vertex.IRVertex;
import edu.snu.nemo.common.ir.vertex.executionproperty.ParallelismProperty;

import java.util.HashMap;
import java.util.Map;

/**
 * Pass for initiating IREdge Metric ExecutionProperty with default key range.
 */
public final class DefaultMetricPass extends AnnotatingPass {
  /**
   * Default constructor.
   */
  public DefaultMetricPass() {
    super(DataSkewMetricProperty.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    dag.topologicalDo(dst ->
      dag.getIncomingEdgesOf(dst).forEach(edge -> {
        if (DataCommunicationPatternProperty.Value.Shuffle
            .equals(edge.getPropertyValue(DataCommunicationPatternProperty.class).get())) {
          final int parallelism = dst.getPropertyValue(ParallelismProperty.class).get();
          final Map<Integer, KeyRange> metric = new HashMap<>();
          for (int i = 0; i < parallelism; i++) {
            metric.put(i, HashRange.of(i, i + 1, false));
          }
          edge.setProperty(DataSkewMetricProperty.of(new DataSkewMetricFactory(metric)));
        }
      }));
    return dag;
  }
}
