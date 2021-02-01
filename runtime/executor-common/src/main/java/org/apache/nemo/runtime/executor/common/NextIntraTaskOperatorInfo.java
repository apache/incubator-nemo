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
package org.apache.nemo.runtime.executor.common;

import org.apache.nemo.common.dag.Edge;
import org.apache.nemo.common.ir.vertex.OperatorVertex;

/**
 * Contains information for next operator:
 * -- edgeIndex: the index of edge to next operator.
 * -- nextOperator: next operator vertex
 * -- watermarkManager: next operator's watermark manager
 *
 * ex)
 * --edge (index 0)--&gt;
 * --edge (index 1)--&gt;  watermarkManager --&gt; nextOperator
 * --edge (index 2)--&gt;
 */
public final class NextIntraTaskOperatorInfo {

  private final Edge edgeInfo;
  private final OperatorVertex nextOperator;

  private long inputTimestamp;

  public NextIntraTaskOperatorInfo(final Edge edgeInfo,
                                   final OperatorVertex nextOperator) {
    this.edgeInfo = edgeInfo;
    this.nextOperator = nextOperator;
  }

  public void setTimestamp(final long timestamp) {
    inputTimestamp = timestamp;
  }

  public long getInputTimestamp() {
    return inputTimestamp;
  }

  public Edge getEdge() {
    return edgeInfo;
  }

  public OperatorVertex getNextOperator() {
    return nextOperator;
  }

}
