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
package org.apache.nemo.runtime.master.metric;

import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Metric message handler.
 */
@DefaultImplementation(MetricManagerMaster.class)
public interface MetricMessageHandler {

  /**
   * Handle the received metric message.
   *
   * @param metricType  a given type for the metric (ex. TaskMetric).
   * @param metricId    id of the metric.
   * @param metricField field name of the metric.
   * @param metricValue serialized metric data value.
   */
  void onMetricMessageReceived(String metricType, String metricId,
                               String metricField, byte[] metricValue);

  /**
   * Cleans up and terminates this handler.
   */
  void terminate();
}
