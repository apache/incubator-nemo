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
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.executionproperty.MinParallelismProperty;
import org.apache.nemo.common.ir.vertex.system.MessageAggregatorVertex;
import org.apache.nemo.common.ir.vertex.system.MessageBarrierVertex;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.common.ir.edge.executionproperty.DecoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;
import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.Annotates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Pass to reshape the IR DAG for skew handling.
 * We insert a {@link MessageBarrierVertex} for each shuffle edge,
 * and aggregate messages for multiple same-destination shuffle edges.
 * */
@Annotates({MetricCollectionProperty.class, PartitionerProperty.class})
@Requires(CommunicationPatternProperty.class)
public final class SkewReshapingPass extends ReshapingPass {
  private static final Logger LOG = LoggerFactory.getLogger(SkewReshapingPass.class.getName());
  private static final String MAIN_OUTPUT_TAG = "MAIN_OUTPUT_TAG";

  /**
   * Hash range multiplier.
   * If we need to split or recombine an output data from a task after it is stored,
   * we multiply the hash range with this factor in advance
   * to prevent the extra deserialize - rehash - serialize process.
   * In these cases, the hash range will be (hash range multiplier X destination task parallelism).
   */
  public static final int HASH_RANGE_MULTIPLIER = 5;

  /**
   * Default constructor.
   */
  public SkewReshapingPass() {
    super(SkewReshapingPass.class);
  }

  @Override
  public IRDAG apply(final IRDAG dag) {
    // TODO #210: Data-aware dynamic optimization at run-time
    dag.topologicalDo(v -> {
      // Incoming shuffle edges grouped by the AdditionalOutputTagProperty.
      final Function<IREdge, String> groupingFunction = irEdge -> {
        return irEdge.getPropertyValue(AdditionalOutputTagProperty.class).orElse(MAIN_OUTPUT_TAG);
      };
      final Map<String, Set<IREdge>> shuffleEdgesGroupedByTag = dag.getIncomingEdgesOf(v).stream()
        .filter(e -> CommunicationPatternProperty.Value.Shuffle
          .equals(e.getPropertyValue(CommunicationPatternProperty.class).get()))
        .collect(Collectors.groupingBy(groupingFunction, Collectors.toSet()));

      // For each shuffle edge group...
      for (final Set<IREdge> shuffleEdgeGroup : shuffleEdgesGroupedByTag.values()) {
        final IREdge representativeEdge = shuffleEdgeGroup.iterator().next();

        // Get the key extractor
        final KeyExtractor keyExtractor = representativeEdge.getPropertyValue(KeyExtractorProperty.class).get();

        // For collecting the data
        final BiFunction<Object, Map<Object, Long>, Map<Object, Long>> dynOptDataCollector =
          (BiFunction<Object, Map<Object, Long>, Map<Object, Long>> & Serializable)
            (element, dynOptData) -> {
              Object key = keyExtractor.extractKey(element);
              if (dynOptData.containsKey(key)) {
                dynOptData.compute(key, (existingKey, existingCount) -> (long) existingCount + 1L);
              } else {
                dynOptData.put(key, 1L);
              }
              return dynOptData;
            };

        // For aggregating the collected data
        final BiFunction<Pair<Object, Long>, Map<Object, Long>, Map<Object, Long>> dynOptDataAggregator =
          (BiFunction<Pair<Object, Long>, Map<Object, Long>, Map<Object, Long>> & Serializable)
            (element, aggregatedDynOptData) -> {
              final Object key = element.left();
              final Long count = element.right();
              if (aggregatedDynOptData.containsKey(key)) {
                aggregatedDynOptData.compute(key, (existingKey, accumulatedCount) -> accumulatedCount + count);
              } else {
                aggregatedDynOptData.put(key, count);
              }
              return aggregatedDynOptData;
            };

        // Coders to use
        final EncoderProperty encoderProperty = EncoderProperty.of(PairEncoderFactory.
          of(representativeEdge.getPropertyValue(KeyEncoderProperty.class).get(), LongEncoderFactory.of()));
        final DecoderProperty decoderProperty = DecoderProperty.of(PairDecoderFactory
          .of(representativeEdge.getPropertyValue(KeyDecoderProperty.class).get(), LongDecoderFactory.of()));

        // Insert the vertices
        final MessageBarrierVertex mbv = new MessageBarrierVertex<>(dynOptDataCollector);
        final MessageAggregatorVertex mav = new MessageAggregatorVertex(new HashMap(), dynOptDataAggregator);
        dag.insert(mbv, mav, encoderProperty, decoderProperty, shuffleEdgeGroup);

        // Set the partitioner property
        final int dstParallelism = representativeEdge.getDst().getPropertyValue(MinParallelismProperty.class).get();
        shuffleEdgeGroup.forEach(e -> {
          e.setPropertyPermanently(PartitionerProperty.of(
            PartitionerProperty.PartitionerType.Hash, dstParallelism * HASH_RANGE_MULTIPLIER));
        });
      }
    });
    return dag;
  }
}

