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
 * Latency mark is conveyor that has data for debugging.
 * It is created only from source vertex and record the timestamp when it is created and taskId where it is created.
 */
public final class Latencymark implements Serializable {
  private final String createdTaskId;
  private final long createdTimestamp;
  private String previousTaskId;
  private long previousSentTimestamp;


  /**
   * @param taskId task id where it is created
   * @param timestamp timestamp when it is created
   */
  public Latencymark(final String taskId, final long timestamp) {
    this.createdTaskId = taskId;
    this.createdTimestamp = timestamp;
    this.previousTaskId = "";
    this.previousSentTimestamp = 0;
  }

  /**
   * @return the latencymark timestamp
   */
  public long getCreatedTimestamp() {
    return createdTimestamp;
  }

  /**
   * @return the task id where it is created
   */
  public String getCreatedTaskId() {
    return createdTaskId;
  }

  /**
   * @return the task id of previous task
   */
  public String getPreviousTaskId() {
    return previousTaskId;
  }

  /**
   * Set the previousTaskId.
   *
   * @param taskId the task id.
   */
  public void setPreviousTaskId(final String taskId) {
    previousTaskId = taskId;
  }

  /**
   * Set the previousSentTimestamp.
   *
   * @param timestamp the timestamp.
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
    final Latencymark latencymark = (Latencymark) o;
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
