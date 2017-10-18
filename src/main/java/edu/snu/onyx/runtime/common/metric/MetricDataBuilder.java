/*
 * Copyright (C) 2017 Seoul National University
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

package edu.snu.onyx.runtime.common.metric;

import java.util.Map;

/**
 * MetricData Builder.
 */
public final class MetricDataBuilder {
  private final String computationUnitId;
  private long startTime;
  private long endTime;
  private Map<String, Object> metrics;

  public MetricDataBuilder(final String computationUnitId) {
    this.computationUnitId = computationUnitId;
    startTime = 0;
    endTime = 0;
    metrics = null;
  }

  public String getComputationUnitId() {
    return computationUnitId;
  }

  public Map<String, Object> getMetrics() {
    return metrics;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void beginMeasurement(final Map<String, Object> metricMap) {
    startTime = System.nanoTime();
    metricMap.put("StartTime", startTime);
    this.metrics = metricMap;
  }

  public void endMeasurement(final Map<String, Object> metricMap) {
    endTime = System.nanoTime();
    metricMap.put("EndTime", endTime);
    metricMap.put("ElapsedTime(s)", (endTime - startTime) / 1000000000);
    this.metrics.putAll(metricMap);
  }

  /**
   * Builds immutable MetricData.
   * @return the MetricData constructed by the builder.
   */
  public MetricData build() {
    return new MetricData(getComputationUnitId(), getMetrics());
  }
}
