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

package org.apache.nemo.runtime.master.scheduler;

import org.apache.nemo.common.Pair;
import org.apache.nemo.runtime.common.RuntimeIdManager;
import org.apache.nemo.runtime.common.plan.StageEdge;

import java.util.*;

/**
 * This is a Simulator for managing network environment.
 * It manages network bandwidth and latency between node pairs.
 */
final class NetworkSimulator {
  private static final long DEFAULT_BANDWIDTH = 1000000; //kbps
  private static final long LOCAL_BANDWIDTH = 1000000; //kbps
  private static final long DEFAULT_LATENCY = 100; // ms
  private static final long LOCAL_LATENCY = 0; // ms

  // hostname1, hostname 2 pair to bandwidth
  private final Map<Pair<String, String>, Long> bandwidthMap;

  // hostname1, hostname 2 pair to latency
  private final Map<Pair<String, String>, Long> latencyMap;

  // runtime edge id, edge index pair to upstream node name.
  private final Map<Pair<String, Integer>, String> runtimeEdgeSrcIndexToNode;

  // runtime edge id, edge index pair to downstream task id.
  private final Map<Pair<String, Integer>, String> runtimeEdgeDstIndexToTaskId;

  NetworkSimulator() {
    bandwidthMap = new HashMap<>();
    latencyMap = new HashMap<>();
    runtimeEdgeDstIndexToTaskId = new HashMap<>();
    runtimeEdgeSrcIndexToNode = new HashMap<>();
  }

  public void reset() {
    bandwidthMap.clear();
    latencyMap.clear();
    runtimeEdgeDstIndexToTaskId.clear();
    runtimeEdgeSrcIndexToNode.clear();
  }

  /**
   * update bandwidthMap.
   */
  public void updateBandwidth(final Map<Pair<String, String>, Long> newBandwidthMap) {
    this.bandwidthMap.putAll(newBandwidthMap);
  }

  /**
   * update latencyMap.
   */
  public void updateLatency(final Map<Pair<String, String>, Long> newLatencyMap) {
    this.latencyMap.putAll(newLatencyMap);
  }

  /**
   * get network bandwidth between two nodes.
   */
  public long getBandwidth(final String nodeName1, final String nodeName2) {
    // If two nodes are the same, It is the data transfer in the same node.
    if (nodeName1.equals(nodeName2)) {
      return LOCAL_BANDWIDTH;
    } else if (bandwidthMap.containsKey(Pair.of(nodeName1, nodeName2))) {
      return bandwidthMap.get(Pair.of(nodeName1, nodeName2));
    } else {
      return bandwidthMap.getOrDefault(Pair.of(nodeName2, nodeName1), DEFAULT_BANDWIDTH);
    }
  }

  /**
   * get network latency between two nodes.
   */
  public long getLatency(final String nodeName1, final String nodeName2) {
    // If two nodes are the same, It is the data transfer in the same node.
    if (nodeName1.equals(nodeName2)) {
      return LOCAL_LATENCY;
    } else if (latencyMap.containsKey(Pair.of(nodeName1, nodeName2))) {
      return latencyMap.get(Pair.of(nodeName1, nodeName2));
    } else {
      return latencyMap.getOrDefault(Pair.of(nodeName2, nodeName1), DEFAULT_LATENCY);
    }
  }

  /**
   * update runtimeEdgeSrcIndexToNode.
   *
   * @param edges outgoing edges from task.
   * @param srcTaskId task id.
   * @param nodeName node name where the task is.
   */
  public void updateRuntimeEdgeSrcIndexToNode(final List<StageEdge> edges,
                                              final String srcTaskId,
                                              final String nodeName) {
    edges.forEach(edge -> {
      final Pair<String, Integer> keyPair =
        Pair.of(edge.getId(), RuntimeIdManager.getIndexFromTaskId(srcTaskId));

      runtimeEdgeSrcIndexToNode.put(keyPair, nodeName);
    });
  }

  /**
   * update runtimeEdgeSrcIndexToNode.
   *
   * @param edges incoming edges of task.
   * @param taskId the task id.
   */
  public void updateRuntimeEdgeDstIndestToTaskId(final List<StageEdge> edges, final String taskId) {
    edges.forEach(edge -> {
      final Pair<String, Integer> keyPair =
        Pair.of(edge.getId(), RuntimeIdManager.getIndexFromTaskId(taskId));

      runtimeEdgeDstIndexToTaskId.put(keyPair, taskId);
    });
  }
  /**
   * get the node name of upstream task by edge id and edge index.
   *
   * @param edgeId edge id.
   * @param dstIndex index of edge.
   *
   * @return node name of upstream task.
   */
  public String getUpstreamNode(final String edgeId, final int dstIndex) {
    return runtimeEdgeSrcIndexToNode.get(Pair.of(edgeId, dstIndex));
  }

  /**
   * get the task id of downstream task by edge id and edge index.
   *
   * @param edgeId edge id.
   * @param srcIndex index of edge.
   *
   * @return task id of downstream task.
   */
  public String getDownstreamTaskId(final String edgeId, final int srcIndex) {
    return runtimeEdgeDstIndexToTaskId.get(Pair.of(edgeId, srcIndex));
  }
}
