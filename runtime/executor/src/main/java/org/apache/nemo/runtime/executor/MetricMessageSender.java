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
package org.apache.nemo.runtime.executor;

import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * Interface for metric sender.
 */
@DefaultImplementation(MetricManagerWorker.class)
public interface MetricMessageSender extends AutoCloseable {

  /**
   * Send metric to master.
   *
   * @param metricType  type of the metric
   * @param metricId    id of the metric
   * @param metricField field of the metric
   * @param metricValue value of the metric which is serialized
   */
  void send(final String metricType, final String metricId, final String metricField, final byte[] metricValue);

  /**
   * Flush all metric inside of the queue.
   */
  void flush();

  /**
   * Flush the metric queue and close the metric dispatch.
   */
  void close();
}
