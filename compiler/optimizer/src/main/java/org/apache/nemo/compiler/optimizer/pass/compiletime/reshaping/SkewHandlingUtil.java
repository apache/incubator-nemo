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
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.DecoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.EncoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.KeyDecoderProperty;
import org.apache.nemo.common.ir.edge.executionproperty.KeyEncoderProperty;

import java.io.Serializable;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * A utility class for skew handling passes.
 */
final class SkewHandlingUtil {
  private SkewHandlingUtil() {
  }

  static BiFunction<Object, Map<Object, Long>, Map<Object, Long>> getDynOptCollector(final KeyExtractor keyExtractor) {
    return (BiFunction<Object, Map<Object, Long>, Map<Object, Long>> & Serializable)
      (element, dynOptData) -> {
        Object key = keyExtractor.extractKey(element);
        if (dynOptData.containsKey(key)) {
          dynOptData.compute(key, (existingKey, existingCount) -> (long) existingCount + 1L);
        } else {
          dynOptData.put(key, 1L);
        }
        return dynOptData;
      };
  }

  static BiFunction<Pair<Object, Long>, Map<Object, Long>, Map<Object, Long>> getDynOptAggregator() {
    return (BiFunction<Pair<Object, Long>, Map<Object, Long>, Map<Object, Long>> & Serializable)
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
  }

  static EncoderProperty getEncoder(final IREdge irEdge) {
    return EncoderProperty.of(PairEncoderFactory
      .of(irEdge.getPropertyValue(KeyEncoderProperty.class).get(), LongEncoderFactory.of()));
  }

  static DecoderProperty getDecoder(final IREdge irEdge) {
    return DecoderProperty.of(PairDecoderFactory
      .of(irEdge.getPropertyValue(KeyDecoderProperty.class).get(), LongDecoderFactory.of()));
  }
}
