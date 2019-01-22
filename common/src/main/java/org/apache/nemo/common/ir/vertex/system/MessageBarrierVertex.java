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
package org.apache.nemo.common.ir.vertex.system;

import org.apache.nemo.common.KeyExtractor;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.KeyExtractorProperty;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.MetricCollectTransform;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class MessageBarrierVertex extends SystemIRVertex {
  public MessageBarrierVertex(final BiFunction<Object, Map<Object, Object>, Map<Object, Object>> messageFunction) {
    super(new MetricCollectTransform(new HashMap<>(), dynOptDataCollector, messageFunction));



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






    final MetricCollectTransform mct
      = new MetricCollectTransform(new HashMap<>(), dynOptDataCollector, closer);
    return new OperatorVertex(mct);


  }

  /**
   * @param edge to collect the metric.
   * @return the generated vertex.
   */
  private OperatorVertex generateMetricCollectVertex(final IREdge edge) {

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

  BiFunction getCloser() {
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

  }




}
