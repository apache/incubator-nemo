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

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.MessageAggregateTransform;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class MessageAggregationVertex extends SystemIRVertex {
  public MessageAggregationVertex() {
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
    final MessageAggregateTransform abt =
      new MessageAggregateTransform<Pair<Object, Long>, Map<Object, Long>>(new HashMap<>(), dynOptDataAggregator);
    return new OperatorVertex(abt);
  }
}
