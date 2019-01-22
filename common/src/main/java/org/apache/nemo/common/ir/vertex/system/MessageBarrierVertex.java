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

import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.MetricCollectTransform;

import java.util.HashMap;
import java.util.function.BiFunction;

public class MessageBarrierVertex implements OperatorVertex {
  public MessageBarrierVertex(final BiFunction messageFunction) {
    super(new MetricCollectTransform(new HashMap<>(), dynOptDataCollector, messageFunction));

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


  

}
