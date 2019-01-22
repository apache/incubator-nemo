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
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.coder.*;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.system.MessageBarrierVertex;
import org.apache.nemo.common.ir.vertex.transform.AggregateMetricTransform;
import org.apache.nemo.common.ir.vertex.transform.MetricCollectTransform;
import org.apache.nemo.compiler.optimizer.PairKeyExtractor;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.Annotates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Pass to reshape the IR DAG for skew handling.
 *
 * This pass inserts vertices to perform two-step dynamic optimization for skew handling.
 * 1) Task-level statistic collection is done via vertex with {@link MetricCollectTransform}
 * 2) Stage-level statistic aggregation is done via vertex with {@link AggregateMetricTransform}
 * inserted before shuffle edges.
 * */
@Annotates(MetricCollectionProperty.class)
@Requires(CommunicationPatternProperty.class)
public final class SkewReshapingPass extends ReshapingPass {
  private static final Logger LOG = LoggerFactory.getLogger(SkewReshapingPass.class.getName());
  private static final String ADDITIONAL_OUTPUT_TAG = "DynOptData";
  /**
   * Default constructor.
   */
  public SkewReshapingPass() {
    super(SkewReshapingPass.class);
  }

  @Override
  public void optimize(final IRDAG dag) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();


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

    final MessageBarrierVertex mbv = new MessageBarrierVertex(dynOptDataCollector);




    dag.topologicalDo(v -> {
      // We care about OperatorVertices that have shuffle incoming edges with main output.
      // TODO #210: Data-aware dynamic optimization at run-time
      if (v instanceof OperatorVertex && dag.getIncomingEdgesOf(v).stream().anyMatch(irEdge ->
          CommunicationPatternProperty.Value.Shuffle
          .equals(irEdge.getPropertyValue(CommunicationPatternProperty.class).get()))
        && dag.getIncomingEdgesOf(v).stream().noneMatch(irEdge ->
      irEdge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())) {

        dag.getIncomingEdgesOf(v).forEach(edge -> {
          if (CommunicationPatternProperty.Value.Shuffle
                .equals(edge.getPropertyValue(CommunicationPatternProperty.class).get())) {
            final OperatorVertex abv = generateMetricAggregationVertex();
            final OperatorVertex mcv = generateMetricCollectVertex(edge);
            builder.addVertex(v);
            builder.addVertex(mcv);
            builder.addVertex(abv);

            // We then insert the vertex with MetricCollectTransform and vertex with AggregateMetricTransform
            // between the vertex and incoming vertices.
            final IREdge edgeToMCV = generateEdgeToMCV(edge, mcv);
            final IREdge edgeToABV = generateEdgeToABV(edge, mcv, abv);
            final IREdge edgeToOriginalDstV =
              new IREdge(edge.getPropertyValue(CommunicationPatternProperty.class).get(), edge.getSrc(), v);
            edge.copyExecutionPropertiesTo(edgeToOriginalDstV);
            edgeToOriginalDstV.setPropertyPermanently(
              MetricCollectionProperty.of(MetricCollectionProperty.Value.DataSkewRuntimePass));

            builder.connectVertices(edgeToMCV);
            builder.connectVertices(edgeToABV);
            builder.connectVertices(edgeToOriginalDstV);

            // Add an control dependency (no output)
            final IREdge emptyEdge =
              new IREdge(CommunicationPatternProperty.Value.BroadCast, abv, v);
            builder.connectVertices(emptyEdge);
          } else {
            builder.connectVertices(edge);
          }
        });
      } else { // Others are simply added to the builder, unless it comes from an updated vertex
        builder.addVertex(v);
        dag.getIncomingEdgesOf(v).forEach(builder::connectVertices);
      }
    });
    final IRDAG newDAG = builder.build();
    return newDAG;
  }

}
