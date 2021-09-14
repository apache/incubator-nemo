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
  private final String createdtaskId;
  private String previousTaskId;
  private final long timestamp;

  /**
   * @param taskId task id where it is created
   * @param timestamp timestamp when it is created
   */
  public Latencymark(final String taskId, final long timestamp) {
    this.createdtaskId = taskId;
    this.timestamp = timestamp;
    this.previousTaskId = "";
  }

  /**
   * @return the latencymark timestamp
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * @return the task id where it is created
   */
  public String getCreatedtaskId() {
    return createdtaskId;
  }


  /**
   * @return the task id of previous task
   */
  public String getPreviousTaskId() {
    return previousTaskId;
  }

  public void setPreviousTaskId(final String currTaskId) {
    previousTaskId = currTaskId;
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
    return (timestamp == latencymark.timestamp)
      && (createdtaskId.equals(latencymark.createdtaskId)
      && (previousTaskId.equals(latencymark.previousTaskId)));
  }


  @Override
  public String toString() {
    return "Latencymark(" + createdtaskId + ", " + timestamp + ")";
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestamp);
  }
}
