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
package edu.snu.vortex.runtime.master;

import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Map;

/**
 * Metric message handler.
 */
@DefaultImplementation(DefaultMetricMessageHandler.class)
public interface MetricMessageHandler {

  /**
   * Handle the received metric message.
   * @param executorId executor id that sends the metric message
   * @param metricData json formatted message object
   */
  void onMetricMessageReceived(String executorId, Map<String, Object> metricData);
}
