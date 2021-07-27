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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.exception.MetricException;
import org.apache.nemo.common.ir.IRDAG;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.runtime.common.plan.Stage;
import org.apache.nemo.runtime.common.plan.StageEdge;
import org.apache.nemo.runtime.common.state.PlanState;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Metric associated with stream. it is periodically recorded.
 */
public class StreamMetric implements Serializable {
  private final String id;
  private long timestamp = -1;
  private Counter numOfProcessedTuples;

  /**
   * Constructor with the designated id.
   *
   * @param id the id.
   */
  public StreamMetric(final String id) {
    this.id = id;
    this.numOfProcessedTuples = new Counter();
  }

  public String getId() {
    return id;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public Counter getNumOfProcessedTuples() {
    return numOfProcessedTuples;
  }

  public void setNumOfProcessedTuples(Counter numOfTuples) {
    this.numOfProcessedTuples = numOfTuples;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
}
