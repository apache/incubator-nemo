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

import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.punctuation.Latencymark;

/**
 * This transform does not emit watermarks.
 * It may be a transform for batch operation that emits collected data when calling {@link Transform#close()}.
 *
 * @param <I> input type
 * @param <O> output type
 */
public abstract class LatencymarkEmitTransform<I, O> implements Transform<I, O> {
  private OutputCollector<O> outputCollector;

  /**
   * @param context context for data transfer.
   * @param oc OutputCollector to transfer data.
   */
  @Override
  public void prepare(final Context context, final OutputCollector<O> oc) {
    this.outputCollector = oc;
  }

  /**
   * get OutputCollector.
   */
  public OutputCollector<O> getOutputCollector() {
    return outputCollector;
  }

  /**
   * @param latencymark latencymark
   */
  @Override
  public final void onLatencymark(final Latencymark latencymark) {
    outputCollector.emitLatencymark(latencymark);
  }
}
