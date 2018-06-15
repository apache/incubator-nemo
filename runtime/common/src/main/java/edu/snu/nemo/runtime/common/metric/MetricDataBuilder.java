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

package edu.snu.nemo.runtime.common.metric;

import java.util.Map;

/**
 * MetricData Builder.
 */
public final class MetricDataBuilder {
  private final String computationUnitId;
  private long startTime;
  private long endTime;
  private Map<String, Object> metrics;

  /**
   * Constructor.
   * @param computationUnitId id of the computation unit.
   */
  public MetricDataBuilder(final String computationUnitId) {
    this.computationUnitId = computationUnitId;
    startTime = 0;
    endTime = 0;
    metrics = null;
  }

  /**
   * @return the id of the computation unit.
   */
  public String getComputationUnitId() {
    return computationUnitId;
  }

  /**
   * @return the metric data.
   */
  public Map<String, Object> getMetrics() {
    return metrics;
  }

  /**
   * @return the time at which metric collection starts.
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * @return the time at which metric collection ends.
   */
  public long getEndTime() {
    return endTime;
  }

  /**
   * Begin the measurement of metric data.
   * @param metricMap map on which to collect metrics.
   */
  public void beginMeasurement(final Map<String, Object> metricMap) {
    startTime = System.currentTimeMillis();
    metricMap.put("StartTime", startTime);
    this.metrics = metricMap;
  }

  /**
   * End the measurement of metric data.
   * @param metricMap map on which to collect metrics.
   */
  public void endMeasurement(final Map<String, Object> metricMap) {
    endTime = System.currentTimeMillis();
    metricMap.put("EndTime", endTime);
    metricMap.put("ElapsedTime(ms)", endTime - startTime);
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
