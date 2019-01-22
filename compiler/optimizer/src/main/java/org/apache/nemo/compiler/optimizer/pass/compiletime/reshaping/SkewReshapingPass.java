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
package org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping;

import org.apache.nemo.common.KeyExtractor;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.system.MessageBarrierVertex;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.Annotates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Pass to reshape the IR DAG for skew handling.
 * We insert a {@link MessageBarrierVertex for each shuffle edge}
 * */
@Annotates(MetricCollectionProperty.class)
@Requires(CommunicationPatternProperty.class)
public final class SkewReshapingPass extends ReshapingPass {
  private static final Logger LOG = LoggerFactory.getLogger(SkewReshapingPass.class.getName());

  /**
   * Default constructor.
   */
  public SkewReshapingPass() {
    super(SkewReshapingPass.class);
  }

  @Override
  public void optimize(final IRDAG dag) {
    dag.topologicalDo(v -> {
      // We care about OperatorVertices that have shuffle incoming edges with main output.
      // TODO #210: Data-aware dynamic optimization at run-time
      for (final IREdge edge : dag.getIncomingEdgesOf(v)) {
        if (CommunicationPatternProperty.Value.Shuffle
          .equals(edge.getPropertyValue(CommunicationPatternProperty.class).get())) {
          // Get the key extractor
          final KeyExtractor keyExtractor = edge.getPropertyValue(KeyExtractorProperty.class).get();

          // Define a custom data collector for skew handling.
          // Here, the collector gathers key frequency data used in shuffle data repartitioning.
          final BiFunction<Object, Map<Object, Object>, Map<Object, Object>> dynOptDataCollector =
            (BiFunction<Object, Map<Object, Object>, Map<Object, Object>> & Serializable)
              (element, dynOptData) -> {
                Object key = keyExtractor.extractKey(element);
                if (dynOptData.containsKey(key)) {
                  dynOptData.compute(key, (existingKey, existingCount) -> (long) existingCount + 1L);
                } else {
                  dynOptData.put(key, 1L);
                }
                return dynOptData;
              };

          final MessageBarrierVertex mbv = new MessageBarrierVertex<>(dynOptDataCollector);

          // Insert the vertex
          dag.insert(mbv, edge);
        }
      }
    });
  }
}
