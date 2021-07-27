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

import java.io.Serializable;

/**
 * Metric class for delay
 */
public class DelayMetric implements Serializable {
  private final String id;
  private long elementCreatedTimestamp;
  private long delay;

  /**
   * Constructor with the designated id, watermark timestamp and delay.
   *
   * @param id the id.
   * @param elementCreatedTimestamp When an element was created.
   * @param delay the time it takes from the time when the element was created to to pass through the current task.
   */
  public DelayMetric(final String id, long elementCreatedTimestamp, long delay) {
    this.id = id;
    this.elementCreatedTimestamp = elementCreatedTimestamp;
    this.delay = delay;
  }

  public String getId() {
    return id;
  }

  public long getElementCreatedTimestamp() {
    return this.elementCreatedTimestamp;
  }

  public void setElementCreatedTimestamp(long watermarkTimestamp) {
    this.elementCreatedTimestamp = watermarkTimestamp;
  }

  public long getDelay() {
    return this.delay;
  }

  public void setDelay(long delay) {
    this.delay = delay;
  }
}
