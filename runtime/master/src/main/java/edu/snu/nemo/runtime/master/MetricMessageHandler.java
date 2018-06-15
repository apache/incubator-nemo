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
package edu.snu.nemo.runtime.master;

import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.List;

/**
 * Metric message handler.
 */
@DefaultImplementation(MetricManagerMaster.class)
public interface MetricMessageHandler {

  /**
   * Handle the received metric message.
   * @param metricKey a given key for the metric (ex. Task ID)
   * @param metricValue the metric formatted as a string (ex. JSON).
   */
  void onMetricMessageReceived(final String metricKey, final String metricValue);

  /**
   * Retrieves the string form of metric given the metric key.
   * @param metricKey to retrieve the metric for
   * @return the list of accumulated metric in string (ex. JSON)
   */
  List<String> getMetricByKey(final String metricKey);

  /**
   * Cleans up and terminates this handler.
   */
  void terminate();
}
