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
package org.apache.nemo.common.punctuation;

import java.io.Serializable;
import java.util.Objects;

/**
 * Latency mark is a watermark with the data related to stream data latencies.
 * It is created only from source vertex, with the data of when (timestamp) and where (taskId) it was created.
 * Later tasks can infer the latency according to the time that the latencyMark arrives to the task.
 * When the latencyMark arrives in a task, it leaves its record with its task id and timestamp, and
 * later tasks can track the itinerary by looking at the recorded previous task id and timestamp.
 */
public final class LatencyMark implements Serializable {
  private final String createdTaskId;
  private final long createdTimestamp;
  private String previousTaskId;
  private long previousSentTimestamp;


  /**
   * @param taskId task id of where it was created
   * @param timestamp timestamp of when it was created
   */
  public LatencyMark(final String taskId, final long timestamp) {
    this.createdTaskId = taskId;
    this.createdTimestamp = timestamp;
    this.previousTaskId = "";
    this.previousSentTimestamp = 0;
  }

  /**
   * @return the timestamp of when this latencyMark was first created.
   */
  public long getCreatedTimestamp() {
    return createdTimestamp;
  }

  /**
   * @return the task id of where it was created.
   */
  public String getCreatedTaskId() {
    return createdTaskId;
  }

  /**
   * @return the task id of task that this latency mark was previously passed on by.
   */
  public String getPreviousTaskId() {
    return previousTaskId;
  }

  /**
   * @return the time stamp when this latency mark was previously passed on by a task.
   */
  public long getPreviousSentTimestamp() {
    return previousSentTimestamp;
  }

  /**
   * Set the previousTaskId.
   *
   * @param taskId the task id of where this latencyMark has gone through previously.
   */
  public void setPreviousTaskId(final String taskId) {
    previousTaskId = taskId;
  }

  /**
   * Set the previousSentTimestamp.
   *
   * @param timestamp the timestamp of when this latencyMark was sent from a previous task.
   */
  public void setPreviousSentTimestamp(final long timestamp) {
    previousSentTimestamp = timestamp;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final LatencyMark latencymark = (LatencyMark) o;
    return (createdTimestamp == latencymark.createdTimestamp)
      && (createdTaskId.equals(latencymark.createdTaskId)
      && (previousTaskId.equals(latencymark.previousTaskId)));
  }


  @Override
  public String toString() {
    return "Latencymark(" + createdTaskId + ", " + createdTimestamp + ")";
  }

  @Override
  public int hashCode() {
    return Objects.hash(createdTimestamp);
  }
}
