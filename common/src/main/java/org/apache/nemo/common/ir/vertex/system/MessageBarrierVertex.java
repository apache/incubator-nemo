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
import org.apache.nemo.common.ir.vertex.transform.MessageBarrierTransform;

import java.util.Map;
import java.util.function.BiFunction;

/**
 * Generates messages.
 * @param <I> input type
 * @param <K> of the output pair.
 * @param <V> of the output pair.
 */
public class MessageBarrierVertex<I, K, V> extends OperatorVertex {
  /**
   * @param messageFunction for producing a message.
   */
  public MessageBarrierVertex(final BiFunction<I, Map<K, V>, Map<K, V>> messageFunction) {
    super(new MessageBarrierTransform<>(messageFunction));
  }
}
