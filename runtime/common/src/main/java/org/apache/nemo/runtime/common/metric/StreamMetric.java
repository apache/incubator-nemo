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
 * Metrics associated with stream. It is periodically recorded.
 */
public final class StreamMetric implements Serializable {
  private final long startTimeStamp;
  private final long endTimeStamp;
  private final long numOfReadTuples;
  private final boolean isReadNotSerializedData;
  private final long serializedReadBytes;


  /**
   * @param startTimeStamp the starting point from which metric is recorded.
   * @param endTimeStamp the endpoint from which metric is recorded.
   * @param numOfTuples the number of tuples processed between starting point and endpoint.
   * @param serializedReadBytes the number of read bytes processed between starting point and endpoint.
   * @param isReadNotSerializedData whether the task read data from local block or not.
   */
  public StreamMetric(final long startTimeStamp,
                      final long endTimeStamp,
                      final long numOfTuples,
                      final long serializedReadBytes,
                      final boolean isReadNotSerializedData) {
    this.startTimeStamp = startTimeStamp;
    this.endTimeStamp = endTimeStamp;
    this.numOfReadTuples = numOfTuples;
    this.isReadNotSerializedData = isReadNotSerializedData;
    this.serializedReadBytes = serializedReadBytes;
  }

  /**
   * Get starting point of record period.
   *
   * @return start timestamp.
   */
  public long getStartTimeStamp() {
    return startTimeStamp;
  }

  /**
   * Get endpoint of record period.
   *
   * @return end timestamp.
   */
  public long getEndTimeStamp() {
    return endTimeStamp;
  }

  /**
   * Get the number of processed tuple.
   *
   * @return number of tuples.
   */
  public long getNumOfProcessedTuples() {
    return numOfReadTuples;
  }

  /**
   * Get the number of read bytes.
   *
   * @return number of read bytes.
   */
  public long getSerializedReadBytes() {
    return serializedReadBytes;
  }

  /**
   * Get a boolean value that indicates whether it read data from local block or not.
   *
   * @return a boolean value that indicates whether it read data form local block or not.
   */
  public boolean getIsReadNotSerializedData() {
    return isReadNotSerializedData;
  }
}
