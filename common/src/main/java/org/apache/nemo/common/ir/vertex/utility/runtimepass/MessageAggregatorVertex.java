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
package org.apache.nemo.common.ir.vertex.utility.runtimepass;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.IdManager;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.MessageIdVertexProperty;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.transform.MessageAggregatorTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Aggregates upstream messages.
 *
 * @param <K> of the input pair.
 * @param <V> of the input pair.
 * @param <O> of the output aggregated message.
 */
public final class MessageAggregatorVertex<K, V, O> extends OperatorVertex {
  private static final Logger LOG = LoggerFactory.getLogger(MessageAggregatorVertex.class.getName());

  /**
   * @param initialStateSupplier for producing the initial state.
   * @param userFunction for aggregating the messages.
   */
  public MessageAggregatorVertex(final InitialStateSupplier<O> initialStateSupplier,
                                 final MessageAggregatorFunction<K, V, O> userFunction) {
    super(new MessageAggregatorTransform<>(initialStateSupplier, userFunction));
    this.setPropertyPermanently(MessageIdVertexProperty.of(IdManager.generateMessageId()));
    this.setProperty(ParallelismProperty.of(1));
  }

  /**
   * Creates the initial aggregated message.
   * @param <O> of the output aggregated message.
   */
  public interface InitialStateSupplier<O> extends Supplier<O>, Serializable {
  }

  /**
   * Aggregates incoming messages.
   * @param <K> of the input pair.
   * @param <V> of the input pair.
   * @param <O> of the output aggregated message.
   */
  public interface MessageAggregatorFunction<K, V, O> extends BiFunction<Pair<K, V>, O, O>, Serializable {
  }
}
