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
 * Metric associated with stream. it is periodically recorded.
 */
public class StreamMetric implements Serializable {
  private final long startTimeStamp;
  private final long endTimeStamp;
  private final long numOfReadTuples;


  /**
   * Constructor with the designated id.
   *
   * @param startTimeStamp the starting point from which metric is recorded.
   * @param endTimeStamp the endpoint from which metric is recorded.
   * @param numOfTuples the number of tuples processed between starting point and endpoint.
   */
  public StreamMetric(long startTimeStamp, long endTimeStamp, long numOfTuples) {
    this.startTimeStamp = startTimeStamp;
    this.endTimeStamp = endTimeStamp;
    this.numOfReadTuples = numOfTuples;
  }

  public long getStartTimeStamp() {
    return startTimeStamp;
  }

  public long getEndTimeStamp() {
    return endTimeStamp;
  }

  public long getNumOfProcessedTuples() {
    return numOfReadTuples;
  }
}
