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

import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.transform.MessageGeneratorTransform;

import java.io.Serializable;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Produces a message for run-time pass.
 *
 * @param <I> input type
 * @param <K> of the output pair.
 * @param <V> of the output pair.
 */
public final class MessageGeneratorVertex<I, K, V> extends OperatorVertex {
  private final MessageGeneratorFunction<I, K, V> messageFunction;

  /**
   * @param messageFunction for producing a message.
   */
  public MessageGeneratorVertex(final MessageGeneratorFunction<I, K, V> messageFunction) {
    super(new MessageGeneratorTransform<>(messageFunction));
    this.messageFunction = messageFunction;
  }

  public MessageGeneratorFunction<I, K, V> getMessageFunction() {
    return messageFunction;
  }

  /**
   * Applied on the input data elements to produce a message.
   *
   * @param <I> input type
   * @param <K> of the output pair.
   * @param <V> of the output pair.
   */
  public interface MessageGeneratorFunction<I, K, V> extends BiFunction<I, Map<K, V>, Map<K, V>>, Serializable {
  }
}
