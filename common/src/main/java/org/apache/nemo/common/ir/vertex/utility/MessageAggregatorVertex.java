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
package org.apache.nemo.common.ir.vertex.utility;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.MessageAggregatorTransform;

import java.util.function.BiFunction;

/**
 * Aggregates upstream messages.
 * @param <K> of the input pair.
 * @param <V> of the input pair.
 * @param <O> of the output aggregated message.
 */
public class MessageAggregatorVertex<K, V, O> extends OperatorVertex {
  /**
   * @param initialState to use.
   * @param userFunction for aggregating the messages.
   */
  public MessageAggregatorVertex(final O initialState, final BiFunction<Pair<K, V>, O, O> userFunction) {
    super(new MessageAggregatorTransform<>(initialState, userFunction));
  }
}
