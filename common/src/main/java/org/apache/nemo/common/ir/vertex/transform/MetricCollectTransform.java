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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;

/**
 * A {@link Transform} that collects task-level statistics used for dynamic optimization.
 * The collected statistics is sent to vertex with {@link AggregateMetricTransform} as a tagged output
 * when this transform is closed.
 *
 * @param <I> input type.
 * @param <O> output type.
 */
public final class MetricCollectTransform<I, O> extends NoWatermarkEmitTransform<I, O> {
  private static final Logger LOG = LoggerFactory.getLogger(MetricCollectTransform.class.getName());
  private OutputCollector<O> outputCollector;
  private O dynOptData;
  private final BiFunction<Object, O, O> dynOptDataCollector;
  private final BiFunction<O, OutputCollector, O> closer;

  /**
   * MetricCollectTransform constructor.
   * @param dynOptData per-task dynamic optimization data.
   * @param dynOptDataCollector that collects the data.
   * @param closer callback function to be invoked when closing the transform.
   */
  public MetricCollectTransform(final O dynOptData,
                                final BiFunction<Object, O, O> dynOptDataCollector,
                                final BiFunction<O, OutputCollector, O> closer) {
    this.dynOptData = dynOptData;
    this.dynOptDataCollector = dynOptDataCollector;
    this.closer = closer;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<O> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final I element) {
    dynOptData = dynOptDataCollector.apply(element, dynOptData);
  }

  @Override
  public void close() {
    closer.apply(dynOptData, outputCollector);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(MetricCollectTransform.class);
    sb.append(":");
    sb.append(super.toString());
    return sb.toString();
  }
}
