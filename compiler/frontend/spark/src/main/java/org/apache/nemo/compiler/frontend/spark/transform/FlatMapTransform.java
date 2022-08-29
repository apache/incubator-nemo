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
package org.apache.nemo.compiler.frontend.spark.transform;

import org.apache.nemo.common.ir.vertex.transform.LatencymarkEmitTransform;
import org.apache.nemo.common.punctuation.Watermark;
import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * Flatmap Transform that flattens each output element after mapping each elements to an iterator.
 *
 * @param <T> input type.
 * @param <U> output type.
 */
public final class FlatMapTransform<T, U> extends LatencymarkEmitTransform<T, U> {
  private final FlatMapFunction<T, U> func;

  /**
   * Constructor.
   *
   * @param func flat map function.
   */
  public FlatMapTransform(final FlatMapFunction<T, U> func) {
    this.func = func;
  }

  @Override
  public void onData(final T element) {
    try {
      func.call(element).forEachRemaining(getOutputCollector()::emit);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onWatermark(final Watermark watermark) {
    getOutputCollector().emitWatermark(watermark);
  }

  @Override
  public void close() {
    // Do nothing in a SparkTransform.
  }
}
