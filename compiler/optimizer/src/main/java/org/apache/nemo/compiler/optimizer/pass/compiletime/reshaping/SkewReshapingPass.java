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
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DecoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.AggregateMetricTransform;
import org.apache.nemo.common.ir.vertex.transform.MetricCollectTransform;
import org.apache.nemo.compiler.optimizer.PairKeyExtractor;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;
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
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    final List<OperatorVertex> metricCollectVertices = new ArrayList<>();

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
            metricCollectVertices.add(mcv);
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

            builder.connectVertices(edgeToMCV);
            builder.connectVertices(edgeToABV);
            builder.connectVertices(edgeToOriginalDstV);
          } else {
            builder.connectVertices(edge);
          }
        });
      } else { // Others are simply added to the builder, unless it comes from an updated vertex
        builder.addVertex(v);
        dag.getIncomingEdgesOf(v).forEach(builder::connectVertices);
      }
    });
    final DAG<IRVertex, IREdge> newDAG = builder.build();
    return newDAG;
  }

  /**
   * @return the generated vertex.
   */
  private OperatorVertex generateMetricAggregationVertex() {
    // Define a custom data aggregator for skew handling.
    // Here, the aggregator gathers key frequency data used in shuffle data repartitioning.
    final BiFunction<Object, Map<Object, Long>, Map<Object, Long>> dynOptDataAggregator =
      (BiFunction<Object, Map<Object, Long>, Map<Object, Long>> & Serializable)
      (element, aggregatedDynOptData) -> {
        final Object key = ((Pair<Object, Long>) element).left();
        final Long count = ((Pair<Object, Long>) element).right();

        final Map<Object, Long> aggregatedDynOptDataMap = (Map<Object, Long>) aggregatedDynOptData;
        if (aggregatedDynOptDataMap.containsKey(key)) {
          aggregatedDynOptDataMap.compute(key, (existingKey, accumulatedCount) -> accumulatedCount + count);
        } else {
          aggregatedDynOptDataMap.put(key, count);
        }
        return aggregatedDynOptData;
      };
    final AggregateMetricTransform abt =
      new AggregateMetricTransform<Pair<Object, Long>, Map<Object, Long>>(new HashMap<>(), dynOptDataAggregator);
    return new OperatorVertex(abt);
  }

  /**
   * @param edge to collect the metric.
   * @return the generated vertex.
   */
  private OperatorVertex generateMetricCollectVertex(final IREdge edge) {
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

    // Define a custom transform closer for skew handling.
    // Here, we emit key to frequency data map type data when closing transform.
    final BiFunction<Map<Object, Object>, OutputCollector, Map<Object, Object>> closer =
      (BiFunction<Map<Object, Object>, OutputCollector, Map<Object, Object>> & Serializable)
        (dynOptData, outputCollector)-> {
          dynOptData.forEach((k, v) -> {
            final Pair<Object, Object> pairData = Pair.of(k, v);
            outputCollector.emit(ADDITIONAL_OUTPUT_TAG, pairData);
          });
          return dynOptData;
        };

    final MetricCollectTransform mct
      = new MetricCollectTransform(new HashMap<>(), dynOptDataCollector, closer);
    return new OperatorVertex(mct);
  }

  /**
   * @param edge the original shuffle edge.
   * @param mcv the vertex with MetricCollectTransform.
   * @return the generated edge to {@code mcv}.
   */
  private IREdge generateEdgeToMCV(final IREdge edge, final OperatorVertex mcv) {
    final IREdge newEdge =
      new IREdge(CommunicationPatternProperty.Value.OneToOne, edge.getSrc(), mcv);
    newEdge.setProperty(EncoderProperty.of(edge.getPropertyValue(EncoderProperty.class).get()));
    newEdge.setProperty(DecoderProperty.of(edge.getPropertyValue(DecoderProperty.class).get()));
    return newEdge;
  }

  /**
   * @param edge the original shuffle edge.
   * @param mcv the vertex with MetricCollectTransform.
   * @param abv the vertex with AggregateMetricTransform.
   * @return the generated egde from {@code mcv} to {@code abv}.
   */
  private IREdge generateEdgeToABV(final IREdge edge,
                                   final OperatorVertex mcv,
                                   final OperatorVertex abv) {
    final IREdge newEdge = new IREdge(CommunicationPatternProperty.Value.Shuffle, mcv, abv);
    newEdge.setProperty(DataStoreProperty.of(DataStoreProperty.Value.LocalFileStore));
    newEdge.setProperty(DataPersistenceProperty.of(DataPersistenceProperty.Value.Keep));
    newEdge.setProperty(DataFlowProperty.of(DataFlowProperty.Value.Pull));
    newEdge.setProperty(KeyExtractorProperty.of(new PairKeyExtractor()));
    newEdge.setProperty(AdditionalOutputTagProperty.of(ADDITIONAL_OUTPUT_TAG));

    // Dynamic optimization handles statistics on key-value data by default.
    // We need to get coders for encoding/decoding the keys to send data to
    // vertex with AggregateMetricTransform.
    if (edge.getPropertyValue(KeyEncoderProperty.class).isPresent()
      && edge.getPropertyValue(KeyDecoderProperty.class).isPresent()) {
      final EncoderFactory keyEncoderFactory = edge.getPropertyValue(KeyEncoderProperty.class).get();
      final DecoderFactory keyDecoderFactory = edge.getPropertyValue(KeyDecoderProperty.class).get();
      newEdge.setProperty(EncoderProperty.of(PairEncoderFactory.of(keyEncoderFactory, LongEncoderFactory.of())));
      newEdge.setProperty(DecoderProperty.of(PairDecoderFactory.of(keyDecoderFactory, LongDecoderFactory.of())));
    } else {
      // If not specified, follow encoder/decoder of the given shuffle edge.
      newEdge.setProperty(EncoderProperty.of(edge.getPropertyValue(EncoderProperty.class).get()));
      newEdge.setProperty(DecoderProperty.of(edge.getPropertyValue(DecoderProperty.class).get()));
    }

    return newEdge;
  }
}
