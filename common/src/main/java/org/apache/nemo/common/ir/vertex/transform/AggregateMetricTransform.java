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
 * A {@link Transform} that aggregates stage-level statistics sent to the master side optimizer
 * for dynamic optimization.
 *
 * @param <I> input type.
 * @param <O> output type.
 */
public final class AggregateMetricTransform<I, O> extends NoWatermarkEmitTransform<I, O> {
  private static final Logger LOG = LoggerFactory.getLogger(AggregateMetricTransform.class.getName());
  private OutputCollector<O> outputCollector;
  private O aggregatedDynOptData;
  private final BiFunction<Object, O, O> dynOptDataAggregator;

  /**
   * Default constructor.
   * @param aggregatedDynOptData per-stage aggregated dynamic optimization data.
   * @param dynOptDataAggregator aggregator to use.
   */
  public AggregateMetricTransform(final O aggregatedDynOptData,
                                  final BiFunction<Object, O, O> dynOptDataAggregator) {
    this.aggregatedDynOptData = aggregatedDynOptData;
    this.dynOptDataAggregator = dynOptDataAggregator;
  }

  @Override
  public void prepare(final Context context, final OutputCollector<O> oc) {
    this.outputCollector = oc;
  }

  @Override
  public void onData(final I element) {
    aggregatedDynOptData = dynOptDataAggregator.apply(element, aggregatedDynOptData);
  }

  @Override
  public void close() {
    outputCollector.emit(aggregatedDynOptData);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(AggregateMetricTransform.class);
    sb.append(":");
    sb.append(super.toString());
    return sb.toString();
  }
}
