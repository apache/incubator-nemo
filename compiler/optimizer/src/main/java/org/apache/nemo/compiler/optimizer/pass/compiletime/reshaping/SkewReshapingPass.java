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
import org.apache.nemo.common.ir.vertex.utility.MessageAggregatorVertex;
import org.apache.nemo.common.ir.vertex.utility.MessageBarrierVertex;
import org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Pass to reshape the IR DAG for skew handling.
 * We insert a {@link MessageBarrierVertex} for each shuffle edge,
 * and aggregate messages for multiple same-destination shuffle edges.
 * */
@Requires(CommunicationPatternProperty.class)
public final class SkewReshapingPass extends ReshapingPass {
  private static final Logger LOG = LoggerFactory.getLogger(SkewReshapingPass.class.getName());
  private static final String MAIN_OUTPUT_TAG = "MAIN_OUTPUT_TAG";

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

        // Insert the vertices
        final MessageBarrierVertex mbv = new MessageBarrierVertex<>(SkewHandlingUtil.getDynOptCollector(keyExtractor));
        final MessageAggregatorVertex mav =
          new MessageAggregatorVertex(new HashMap(), SkewHandlingUtil.getDynOptAggregator());
        dag.insert(mbv, mav, SkewHandlingUtil.getEncoder(representativeEdge),
          SkewHandlingUtil.getDecoder(representativeEdge), shuffleEdgeGroup, shuffleEdgeGroup);
      }
    });
    return dag;
  }
}

