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
package org.apache.nemo.common.ir.vertex.transform;

import org.apache.nemo.common.Pair;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.utility.runtimepass.MessageGeneratorVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@link Transform} for the message generator vertex.
 *
 * @param <I> input type.
 * @param <K> output key type.
 * @param <V> output value type.
 */
public final class MessageGeneratorTransform<I, K, V> extends NoWatermarkEmitTransform<I, Pair<K, V>> {
  private static final Logger LOG = LoggerFactory.getLogger(MessageGeneratorTransform.class.getName());

  private transient OutputCollector<Pair<K, V>> outputCollector;
  private transient Map<K, V> holder;

  private final MessageGeneratorVertex.MessageGeneratorFunction<I, K, V> userFunction;

  /**
   * TriggerTransform constructor.
   *
   * @param userFunction that analyzes the data.
   */
  public MessageGeneratorTransform(final MessageGeneratorVertex.MessageGeneratorFunction<I, K, V> userFunction) {
    this.userFunction = userFunction;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<Pair<K, V>> oc) {
    this.outputCollector = oc;
    this.holder = new HashMap<>();
  }

  @Override
  public void onData(final I element) {
    holder = userFunction.apply(element, holder);
  }

  @Override
  public void close() {
    holder.forEach((k, v) -> {
      final Pair<K, V> pairData = Pair.of(k, v);
      outputCollector.emit(pairData);
    });
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(MessageGeneratorTransform.class);
    sb.append(":");
    sb.append(super.toString());
    return sb.toString();
  }
}
