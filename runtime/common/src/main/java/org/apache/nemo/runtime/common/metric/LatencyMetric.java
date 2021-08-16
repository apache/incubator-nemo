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
package org.apache.nemo.runtime.common.metric;

import org.apache.nemo.common.punctuation.Latencymark;

import java.io.Serializable;

/**
 * Metric class for latency.
 */
public class LatencyMetric implements Serializable {
  private Latencymark latencymark;
  private long timestamp;

  /**
   * Constructor with the latencymark and timestamp.
   *
   * @param latencymark the latencymark what task received.
   * @param timestamp When the latencymark was received.
   */
  public LatencyMetric(final Latencymark latencymark, final long timestamp) {
    this.latencymark = latencymark;
    this.timestamp = timestamp;
  }

  /**
   * Get the recorded latency mark.
   *
   * @return latency mark.
   */
  public Latencymark getLatencymark() {
    return latencymark;
  }

  /**
   * Get the timestamp when the latencymark is received.
   *
   * @return timestamp when it is received.
   */
  public long getTimestamp() {
    return this.timestamp;
  }
}
