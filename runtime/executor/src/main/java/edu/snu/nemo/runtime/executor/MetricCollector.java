/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.nemo.runtime.executor;

import edu.snu.nemo.runtime.common.metric.MetricDataBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * This metric collector collects metrics and send through {@link MetricMessageSender}.
 */
public final class MetricCollector {

  private final MetricMessageSender metricMessageSender;
  private final Map<String, MetricDataBuilder> metricDataBuilderMap;

  /**
   * Constructor.
   *
   * @param metricMessageSender the metric message sender.
   */
  public MetricCollector(final MetricMessageSender metricMessageSender) {
    this.metricMessageSender = metricMessageSender;
    this.metricDataBuilderMap = new HashMap<>();
  }

  /**
   * Begins recording the start time of this metric measurement, in addition to the metric given.
   * This method ensures thread-safety by synchronizing its callers.
   *
   * @param compUnitId    to be used as metricKey
   * @param initialMetric metric to add
   */
  public void beginMeasurement(final String compUnitId, final Map<String, Object> initialMetric) {
    final MetricDataBuilder metricDataBuilder = new MetricDataBuilder(compUnitId);
    metricDataBuilder.beginMeasurement(initialMetric);
    metricDataBuilderMap.put(compUnitId, metricDataBuilder);
  }

  /**
   * Ends this metric measurement, recording the end time in addition to the metric given.
   * This method ensures thread-safety by synchronizing its callers.
   *
   * @param compUnitId  to be used as metricKey
   * @param finalMetric metric to add
   */
  public void endMeasurement(final String compUnitId, final Map<String, Object> finalMetric) {
    final MetricDataBuilder metricDataBuilder = metricDataBuilderMap.get(compUnitId);
    metricDataBuilder.endMeasurement(finalMetric);
    metricMessageSender.send(compUnitId, metricDataBuilder.build().toJson());
    metricDataBuilderMap.remove(compUnitId);
  }
}
