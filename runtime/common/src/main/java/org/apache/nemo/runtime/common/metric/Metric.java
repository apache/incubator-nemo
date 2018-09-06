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
package org.apache.nemo.runtime.common.metric;

/**
 * Interface for all metrics.
 */
public interface Metric {
  /**
   * Get its unique id.
   * @return an unique id
   */
  String getId();

  /**
   * Process metric message from evaluators.
   * @param metricField field name of the metric.
   * @param metricValue byte array of serialized data value.
   * @return true if the metric was changed or false if not.
   */
  boolean processMetricMessage(final String metricField, final byte[] metricValue);
}
